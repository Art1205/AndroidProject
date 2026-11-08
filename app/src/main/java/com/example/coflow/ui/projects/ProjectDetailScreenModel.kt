package com.example.coflow.ui.projects

import androidx.lifecycle.SavedStateHandle
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.example.coflow.R
import com.example.coflow.models.Role
import com.example.coflow.repositories.AuthRepository
import com.example.coflow.repositories.FirebaseAuthRepositoryImpl
import com.example.coflow.repositories.FirebaseMembershipRepositoryImpl
import com.example.coflow.repositories.FirebaseProjectRepositoryImpl
import com.example.coflow.repositories.FirebaseUserRepositoryImpl
import com.example.coflow.repositories.MembershipRepository
import com.example.coflow.repositories.ProjectRepository
import com.example.coflow.repositories.UserRepository
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch

// --- UI МОДЕЛИ ---

data class ProjectParticipantUi(
    val uid: String,
    val name: String,
    val email: String,
    val initials: String,
    val role: Role,
)

enum class ProjectStatusUi {
    IN_PROGRESS, // В работе
    ARCHIVED     // Архив
}

data class ProjectDetailUiState(
    val projectId: String = "",
    val title: String = "",
    val description: String = "",
    val status: ProjectStatusUi = ProjectStatusUi.IN_PROGRESS,
    val ownerName: String = "",

    val participants: List<ProjectParticipantUi> = emptyList(),
    val searchQuery: String = "",

    val canEdit: Boolean = false,
    val isLoading: Boolean = true,
    val errorMessage: String? = null,      // Для динамических ошибок
    val errorMessageResId: Int? = null,    // Для ошибок из R.string (ДОБАВЛЕНО)
) {
    val filteredParticipants: List<ProjectParticipantUi>
        get() = if (searchQuery.isBlank()) {
            participants
        } else {
            participants.filter {
                it.name.contains(searchQuery, ignoreCase = true) ||
                        it.email.contains(searchQuery, ignoreCase = true)
            }
        }
}

class ProjectDetailScreenViewModel(
    savedStateHandle: SavedStateHandle,
    private val projectRepository: ProjectRepository = FirebaseProjectRepositoryImpl(),
    private val membershipRepository: MembershipRepository = FirebaseMembershipRepositoryImpl(),
    private val userRepository: UserRepository = FirebaseUserRepositoryImpl(),
    private val authRepository: AuthRepository = FirebaseAuthRepositoryImpl(),
) : ViewModel() {

    private val _uiEvent = Channel<UiEvent>()
    val uiEvent = _uiEvent.receiveAsFlow()

    private val _state = MutableStateFlow(ProjectDetailUiState())
    val state: StateFlow<ProjectDetailUiState> = _state.asStateFlow()

    private val projectId: String = savedStateHandle.get<String>("projectId").orEmpty()

    init {
        if (projectId.isNotBlank()) {
            loadProject()
        } else {
            // ИСПРАВЛЕНИЕ 1: Убран хардкод, используем ресурс
            _state.update {
                it.copy(
                    isLoading = false,
                    errorMessageResId = R.string.error_task_id_missing // или создать error_project_id_missing
                )
            }
        }
    }

    private fun loadProject() {
        viewModelScope.launch {
            _state.update { it.copy(isLoading = true, errorMessage = null, errorMessageResId = null) }
            val currentUid = authRepository.getCurrentUserUid().orEmpty()

            // ИСПРАВЛЕНИЕ 3: Оборачиваем в try/catch с обработкой CancellationException
            try {
                // 1. Загружаем сам проект
                val projectResult = projectRepository.getById(projectId)
                val project = projectResult.getOrElse { error ->
                    _state.update { it.copy(isLoading = false, errorMessage = error.message) }
                    return@launch
                }

                // ИСПРАВЛЕНИЕ 2: Запрашиваем владельца ОДИН раз и с проверкой на blank
                val ownerUser = if (project.ownerId.isNotBlank()) {
                    userRepository.getById(project.ownerId).getOrNull()
                } else null

                val ownerName = ownerUser?.name.orEmpty()

                // 3. Загружаем участников (memberships)
                val memberships = membershipRepository.getByProjectId(projectId).getOrDefault(emptyList())

                val participantsList = mutableListOf<ProjectParticipantUi>()

                // Добавляем владельца в список (используем ownerUser из шага 2)
                if (ownerUser != null) {
                    participantsList.add(
                        ProjectParticipantUi(
                            uid = ownerUser.uid,
                            name = ownerUser.name,
                            email = ownerUser.email,
                            initials = getInitials(ownerUser.name),
                            role = Role.OWNER
                        )
                    )
                }

                // Добавляем остальных участников
                for (membership in memberships) {
                    if (membership.userId == project.ownerId) continue

                    val user = userRepository.getById(membership.userId).getOrNull()
                    if (user != null) {
                        participantsList.add(
                            ProjectParticipantUi(
                                uid = user.uid,
                                name = user.name,
                                email = user.email,
                                initials = getInitials(user.name),
                                role = membership.role
                            )
                        )
                    }
                }

                val projectStatus = if (project.isDeleted) ProjectStatusUi.ARCHIVED else ProjectStatusUi.IN_PROGRESS

                // 4. Обновляем стейт
                _state.update {
                    it.copy(
                        projectId = project.id,
                        title = project.title,
                        description = project.description,
                        status = projectStatus,
                        ownerName = ownerName,
                        participants = participantsList,
                        canEdit = (project.ownerId == currentUid),
                        isLoading = false
                    )
                }
            } catch (e: CancellationException) {
                throw e // <-- Пробрасываем отмену дальше, чтобы не показать ложную ошибку
            } catch (e: Exception) {
                _state.update {
                    it.copy(
                        isLoading = false,
                        errorMessage = e.message,
                        errorMessageResId = R.string.error_system
                    )
                }
            }
        }
    }

    fun onSearchQueryChange(query: String) {
        _state.update { it.copy(searchQuery = query) }
    }

    fun onEditClick() {
        // Заглушка
    }

    fun onBackClick() {
        viewModelScope.launch { _uiEvent.send(UiEvent.NavigateBack) }
    }

    fun clearError() {
        _state.update { it.copy(errorMessage = null, errorMessageResId = null) }
    }

    private fun getInitials(name: String): String {
        val parts = name.trim().split("\\s+".toRegex()).filter { it.isNotBlank() }
        return when {
            parts.isEmpty() -> "??"
            parts.size == 1 -> parts[0].take(2).uppercase()
            else -> (parts[0].take(1) + parts[1].take(1)).uppercase()
        }
    }

    sealed class UiEvent {
        object NavigateBack : UiEvent()
    }
}