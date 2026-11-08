package com.example.coflow.ui.projects

import androidx.lifecycle.SavedStateHandle
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.example.coflow.models.Role
import com.example.coflow.repositories.AuthRepository
import com.example.coflow.repositories.FirebaseAuthRepositoryImpl
import com.example.coflow.repositories.FirebaseMembershipRepositoryImpl
import com.example.coflow.repositories.FirebaseProjectRepositoryImpl
import com.example.coflow.repositories.FirebaseUserRepositoryImpl
import com.example.coflow.repositories.MembershipRepository
import com.example.coflow.repositories.ProjectRepository
import com.example.coflow.repositories.UserRepository
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch

// TADO: добавить вызов окна редактирования проекта

// Модель участника для списка на экране
data class ProjectParticipantUi(
    val uid: String,
    val name: String,
    val email: String,
    val initials: String,
    val role: Role,
)

// Статус проекта
enum class ProjectStatusUi {
    IN_PROGRESS, // В работе
    ARCHIVED     // Архив
}

// Главный стейт экрана
data class ProjectDetailUiState(
    val projectId: String = "",
    val title: String = "",
    val description: String = "",
    val status: ProjectStatusUi = ProjectStatusUi.IN_PROGRESS,
    val ownerName: String = "",

    // Участники
    val participants: List<ProjectParticipantUi> = emptyList(),
    val searchQuery: String = "",

    // Доступ и состояния
    val canEdit: Boolean = false,
    val isLoading: Boolean = true,
    val errorMessage: String? = null,
) {
    // Вычисляемое свойство: отфильтрованный список участников по имени ИЛИ почте
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

// --- VIEWMODEL ---

class ProjectDetailScreenViewModel(
    savedStateHandle: SavedStateHandle,
    private val projectRepository: ProjectRepository = FirebaseProjectRepositoryImpl(),
    private val membershipRepository: MembershipRepository = FirebaseMembershipRepositoryImpl(),
    private val userRepository: UserRepository = FirebaseUserRepositoryImpl(),
    private val authRepository: AuthRepository = FirebaseAuthRepositoryImpl(),
) : ViewModel() {

    // События для UI (например, навигация)
    private val _uiEvent = Channel<UiEvent>()
    val uiEvent = _uiEvent.receiveAsFlow()

    private val _state = MutableStateFlow(ProjectDetailUiState())
    val state: StateFlow<ProjectDetailUiState> = _state.asStateFlow()

    private val projectId: String = savedStateHandle.get<String>("projectId").orEmpty()

    init {
        if (projectId.isNotBlank()) {
            loadProject()
        } else {
            _state.update { it.copy(isLoading = false, errorMessage = "ID проекта не передан") }
        }
    }

    private fun loadProject() {
        viewModelScope.launch {
            _state.update { it.copy(isLoading = true, errorMessage = null) }
            val currentUid = authRepository.getCurrentUserUid().orEmpty()

            // 1. Загружаем сам проект
            val projectResult = projectRepository.getById(projectId)
            val project = projectResult.getOrElse { error ->
                _state.update { it.copy(isLoading = false, errorMessage = error.message) }
                return@launch
            }

            // 2. Загружаем имя создателя (владельца)
            val ownerName = if (project.ownerId.isNotBlank()) {
                userRepository.getById(project.ownerId).getOrNull()?.name.orEmpty()
            } else ""

            // 3. Загружаем участников (memberships)
            val memberships = membershipRepository.getByProjectId(projectId).getOrDefault(emptyList())

            val participantsList = mutableListOf<ProjectParticipantUi>()

            // Добавляем владельца в список (если его там еще нет)
            val ownerUser = userRepository.getById(project.ownerId).getOrNull()
            if (ownerUser != null) {
                participantsList.add(
                    ProjectParticipantUi(
                        uid = ownerUser.uid,
                        name = ownerUser.name,
                        email = ownerUser.email,
                        initials = getInitials(ownerUser.name),
                        role = Role.OWNER // Владелец = Админ
                    )
                )
            }

            // Добавляем остальных участников
            for (membership in memberships) {
                // Пропускаем, если это владелец (мы уже добавили)
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

            // Определяем статус на основе isDeleted
            val projectStatus = if (project.isDeleted) ProjectStatusUi.ARCHIVED else ProjectStatusUi.IN_PROGRESS

            // 4. Обновляем стейт
            _state.update {
                it.copy(
                    projectId = project.id,
                    title = project.title,
                    description = project.description,
                    status = projectStatus, // Привязали к isDeleted
                    ownerName = ownerName,
                    participants = participantsList,
                    canEdit = (project.ownerId == currentUid), // Только владелец может редактировать
                    isLoading = false
                )
            }
        }
    }

    // --- Обработчики действий ---

    fun onSearchQueryChange(query: String) {
        _state.update { it.copy(searchQuery = query) }
    }

    fun onEditClick() {
        // Заглушка: окно редактирования еще не сделано.
        // Когда дизайнеры его сделают, здесь будет навигация.
    }

    fun onBackClick() {
        viewModelScope.launch { _uiEvent.send(UiEvent.NavigateBack) }
    }

    fun clearError() {
        _state.update { it.copy(errorMessage = null) }
    }

    // Вспомогательная функция для аватарки
    private fun getInitials(name: String): String {
        val parts = name.trim().split("\\s+".toRegex()).filter { it.isNotBlank() }
        return when {
            parts.isEmpty() -> "??"
            parts.size == 1 -> parts[0].take(2).uppercase()
            else -> (parts[0].take(1) + parts[1].take(1)).uppercase()
        }
    }

    // События навигации
    sealed class UiEvent {
        object NavigateBack : UiEvent()
    }
}



