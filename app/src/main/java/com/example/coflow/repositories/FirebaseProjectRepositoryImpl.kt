package com.example.coflow.repositories

import com.example.coflow.models.ProjectModel
import com.example.coflow.repositories.dto.ProjectAddDto
import com.example.coflow.repositories.dto.ProjectUpdateDto
import com.google.firebase.Timestamp
import com.google.firebase.firestore.CollectionReference
import com.google.firebase.firestore.DocumentSnapshot
import com.google.firebase.firestore.FirebaseFirestore
import com.google.firebase.firestore.FirebaseFirestoreException
import com.google.firebase.firestore.ListenerRegistration
import com.google.firebase.firestore.QuerySnapshot
import kotlinx.coroutines.channels.ProducerScope
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.tasks.await
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * Firestore-реализация проектов.
 *
 * Коллекция `projects`; `ownerId` — строка, `updatedAt` — Timestamp,
 * `isDeleted` — Boolean. getByUserId() дополнительно читает `memberships`.
 * observeByUserId() слушает изменения проектов и членства в реальном времени.
 * updateProject() меняет только переданные поля DTO и обновляет updatedAt.
 * addProject() принимает ProjectAddDto и сам генерирует ID.
 */
class FirebaseProjectRepositoryImpl(
    private val db: FirebaseFirestore = FirebaseFirestore.getInstance(),
) : ProjectRepository {
    private val projectCollection: CollectionReference = db.collection("projects")
    private val membershipCollection: CollectionReference = db.collection("memberships")

    override suspend fun getById(id: String): Result<ProjectModel> =
        try {
            val project =
                projectCollection
                    .document(id)
                    .get()
                    .await()
                    .toProjectModel()
                    ?: throw Exception("Проект не найден")
            Result.success(project)
        } catch (e: Exception) {
            Result.failure(Exception(e.message ?: "Ошибка загрузки проекта"))
        }

    override suspend fun getByOwnerId(ownerId: String): Result<List<ProjectModel>> =
        try {
            val documents =
                projectCollection
                    .whereEqualTo("ownerId", ownerId)
                    .get()
                    .await()
                    .documents
            Result.success(documents.mapNotNull { it.toProjectModel() }.filterNot { it.isDeleted })
        } catch (e: Exception) {
            Result.failure(Exception(e.message ?: "Ошибка загрузки проектов владельца"))
        }

    override suspend fun getByUserId(userId: String): Result<List<ProjectModel>> =
        try {
            val memberships =
                membershipCollection
                    .whereEqualTo("userId", userId)
                    .get()
                    .await()
                    .documents

            val projectIds =
                memberships
                    .mapNotNull { it.getString("projectId") }
                    .filter { it.isNotBlank() }
                    .distinct()

            val projects =
                projectIds.mapNotNull { projectId ->
                    projectCollection
                        .document(projectId)
                        .get()
                        .await()
                        .toProjectModel()
                }

            Result.success(projects.filterNot { it.isDeleted })
        } catch (e: Exception) {
            Result.failure(Exception(e.message ?: "Ошибка загрузки проектов пользователя"))
        }

    /** Состояние одной подписки на проекты пользователя. */
    private class ProjectObservationState {
        val ownedProjects = linkedMapOf<String, ProjectModel>()
        val memberProjects = linkedMapOf<String, ProjectModel>()
        val memberProjectRegistrations = mutableMapOf<String, ListenerRegistration>()
    }

    /** Отправляет в поток актуальный список проектов. */
    private fun ProducerScope<Result<List<ProjectModel>>>.emitProjects(state: ProjectObservationState) {
        val projects =
            (state.ownedProjects.values + state.memberProjects.values)
                .distinctBy { it.id }
                .filterNot { it.isDeleted }
                .sortedByDescending { it.updatedAt }
        trySend(Result.success(projects))
    }

    /** Синхронизирует подписки на проекты, в которых пользователь состоит. */
    private fun ProducerScope<Result<List<ProjectModel>>>.syncMemberProjects(
        projectIds: Set<String>,
        state: ProjectObservationState,
    ) {
        (state.memberProjectRegistrations.keys - projectIds).forEach { projectId ->
            state.memberProjectRegistrations.remove(projectId)?.remove()
            state.memberProjects.remove(projectId)
        }

        projectIds.forEach { projectId ->
            if (projectId !in state.memberProjectRegistrations) {
                state.memberProjectRegistrations[projectId] =
                    projectCollection.document(projectId).addSnapshotListener { snapshot, error ->
                        if (error != null) {
                            trySend(
                                Result.failure(
                                    Exception(error.message ?: "Ошибка загрузки проекта"),
                                ),
                            )
                            return@addSnapshotListener
                        }

                        val project = snapshot?.toProjectModel()
                        if (project == null) {
                            state.memberProjects.remove(projectId)
                        } else {
                            state.memberProjects[projectId] = project
                        }
                        emitProjects(state)
                    }
            }
        }
    }

    /** Обрабатывает изменения членства пользователя. */
    private fun ProducerScope<Result<List<ProjectModel>>>.onMembershipsSnapshot(
        snapshot: QuerySnapshot?,
        error: FirebaseFirestoreException?,
        state: ProjectObservationState,
    ) {
        if (error != null) {
            trySend(
                Result.failure(
                    Exception(error.message ?: "Ошибка загрузки членства"),
                ),
            )
            return
        }

        val projectIds =
            snapshot?.documents
                ?.mapNotNull { it.getString("projectId") }
                ?.filter { it.isNotBlank() }
                ?.toSet()
                .orEmpty()

        syncMemberProjects(projectIds, state)
        emitProjects(state)
    }

    /** Обрабатывает изменения проектов, где пользователь является владельцем. */
    private fun ProducerScope<Result<List<ProjectModel>>>.onOwnedProjectsSnapshot(
        snapshot: QuerySnapshot?,
        error: FirebaseFirestoreException?,
        state: ProjectObservationState,
    ) {
        if (error != null) {
            trySend(
                Result.failure(
                    Exception(error.message ?: "Ошибка загрузки проектов владельца"),
                ),
            )
            return
        }

        state.ownedProjects.clear()
        snapshot?.documents?.forEach { document ->
            document.toProjectModel()?.let { project ->
                state.ownedProjects[project.id] = project
            }
        }
        emitProjects(state)
    }

    override fun observeByUserId(id: String): Flow<Result<List<ProjectModel>>> =
        callbackFlow {
            val state = ProjectObservationState()

            val membershipsRegistration =
                membershipCollection
                    .whereEqualTo("userId", id)
                    .addSnapshotListener { snapshot, error ->
                        onMembershipsSnapshot(snapshot, error, state)
                    }

            val ownedProjectsRegistration =
                projectCollection
                    .whereEqualTo("ownerId", id)
                    .addSnapshotListener { snapshot, error ->
                        onOwnedProjectsSnapshot(snapshot, error, state)
                    }

            awaitClose {
                membershipsRegistration.remove()
                ownedProjectsRegistration.remove()
                state.memberProjectRegistrations.values.forEach { it.remove() }
                state.memberProjectRegistrations.clear()
            }
        }

    override suspend fun addProject(project: ProjectAddDto): Result<ProjectModel> =
        try {
            require(project.ownerId.isNotBlank()) { "ownerId обязателен" }

            val projectDocument = projectCollection.document()
            val saved =
                ProjectModel(
                    id = projectDocument.id,
                    ownerId = project.ownerId,
                    title = project.title,
                    description = project.description,
                    updatedAt = LocalDateTime.now(ZoneOffset.UTC),
                    isDeleted = false,
                )

            projectDocument.set(saved.toFirestoreMap()).await()

            val created =
                projectDocument.get().await().toProjectModel()
                    ?: throw Exception("Не удалось прочитать созданный проект")
            Result.success(created)
        } catch (e: Exception) {
            Result.failure(Exception(e.message ?: "Ошибка создания проекта"))
        }

    override suspend fun updateProject(
        id: String,
        update: ProjectUpdateDto,
    ): Result<ProjectModel> =
        try {
            require(id.isNotBlank()) { "ID проекта не задан" }

            val updates = update.toUpdateMap()
            require(updates.isNotEmpty()) { "Нет полей для обновления" }

            val projectDocument = projectCollection.document(id)
            projectDocument.update(updates).await()

            val updated =
                projectDocument.get().await().toProjectModel()
                    ?: throw Exception("Проект не найден")
            Result.success(updated)
        } catch (e: Exception) {
            Result.failure(Exception(e.message ?: "Ошибка обновления проекта"))
        }

    override suspend fun deleteProject(id: String): Result<Unit> =
        try {
            require(id.isNotBlank()) { "ID проекта не задан" }

            // Мягкое удаление: документ остаётся, но пропадает из списков.
            projectCollection
                .document(id)
                .update(
                    mapOf(
                        "isDeleted" to true,
                        "updatedAt" to LocalDateTime.now(ZoneOffset.UTC).toTimestamp(),
                    ),
                ).await()
            Result.success(Unit)
        } catch (e: Exception) {
            Result.failure(Exception(e.message ?: "Ошибка удаления проекта"))
        }
}

private fun ProjectModel.toFirestoreMap(): Map<String, Any> =
    mapOf(
        "ownerId" to ownerId,
        "title" to title,
        "description" to description,
        "updatedAt" to updatedAt.toTimestamp(),
        "isDeleted" to isDeleted,
    )

private fun ProjectUpdateDto.toUpdateMap(): Map<String, Any> =
    buildMap {
        title?.let { put("title", it) }
        description?.let { put("description", it) }
        if (isNotEmpty()) {
            put("updatedAt", LocalDateTime.now(ZoneOffset.UTC).toTimestamp())
        }
    }

private fun LocalDateTime.toTimestamp(): Timestamp {
    val instant = toInstant(ZoneOffset.UTC)
    return Timestamp(instant.epochSecond, instant.nano)
}

private fun DocumentSnapshot.toProjectModel(): ProjectModel? {
    if (!exists()) return null

    val updatedAt =
        getTimestamp("updatedAt")?.let {
            LocalDateTime.ofInstant(
                Instant.ofEpochSecond(it.seconds, it.nanoseconds.toLong()),
                ZoneOffset.UTC,
            )
        } ?: LocalDateTime.now(ZoneOffset.UTC)

    return ProjectModel(
        id = id,
        ownerId = getString("ownerId") ?: "",
        title = getString("title") ?: "",
        description = getString("description") ?: "",
        updatedAt = updatedAt,
        isDeleted = getBoolean("isDeleted") ?: false,
    )
}
