import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

import { useNeo4jCore } from '@/composables/useNeo4jCore'
import { useRoute } from '@/composables/useRoute'

/**
 * Enumeration for Neo4j application statuses.
 * @readonly
 * @enum {string}
 */
export const AppStatus = {
  Error: 'Error',
  Running: 'Running',
  Starting: 'Starting',
  Stopped: 'Stopped'
}

/**
 * Enumeration for Neo4j task statuses.
 * @readonly
 * @enum {string}
 */
export const TaskStatus = {
  Error: 'ERROR',
  Done: 'DONE',
  Running: 'RUNNING',
  Queued: 'QUEUED',
  Retry: 'RETRY',
  Created: 'CREATED',
  Cancelled: 'CANCELLED'
}

/**
 * Neo4j store for managing Neo4j status, tasks, and graph counts.
 *
 * This store exposes individual state variables, computed getters, and actions
 * that update those state variables. All former Vuex mutations are now directly
 * implemented as actions.
 */
export const useNeo4jStore = defineStore('neo4j', () => {
  const route = useRoute()
  const neo4jCore = useNeo4jCore()

  /**
   * Current Neo4j application status.
   * @type {import('vue').Ref<string>}
   */
  const status = ref(AppStatus.Stopped)

  /**
   * Dump limit returned from Neo4j.
   * @type {import('vue').Ref<number|null>}
   */
  const dumpLimit = ref(null)

  /**
   * Map of projects to their Neo4j initialization status.
   * @type {import('vue').Ref<Object>}
   */
  const initializedProjects = ref({})

  /**
   * The current set of Neo4j import tasks.
   * @type {import('vue').Ref<any>}
   */
  const syncTasks = ref(null)

  /**
   * Graph counts per project.
   * @type {import('vue').Ref<Object>}
   */
  const graphCounts = ref({})

  /**
   * Returns whether the Neo4j application is running.
   * @returns {boolean}
   */
  const isRunning = computed(() => status.value === AppStatus.Running)

  /**
   * Returns all import tasks that are pending.
   * @returns {Array}
   */
  const pendingImportTasks = computed(() => syncTasks.value.filter(isTaskPending))

  /**
   * Checks if a task is pending based on its status.
   * @param {Object} task - The task object.
   * @param {string} task.status - The status of the task.
   * @returns {boolean} - True if the task is pending, false otherwise.
   */
  function isTaskPending({ status }) {
    return [TaskStatus.Queued, TaskStatus.Created, TaskStatus.Retry, TaskStatus.Running].includes(status)
  }

  /**
   * Sets a new dump limit.
   * @param {number} newDumpLimit - The new dump limit value.
   */
  function setDumpLimit(newDumpLimit) {
    dumpLimit.value = newDumpLimit
  }

  /**
   * Sets the Neo4j import tasks.
   * @param {any} tasks - The new import tasks.
   */
  function setImportTasks(tasks) {
    syncTasks.value = tasks
  }

  /**
   * Sets the graph counts for a given project.
   *
   * @param {Object} payload
   * @param {string} payload.project - The project identifier.
   * @param {any} payload.counts - The graph counts for the project.
   */
  function setGraphCounts({ project, counts }) {
    graphCounts.value[project] = counts
  }

  /**
   * Sets the Neo4j application status.
   *
   * @param {string} newStatus - The new application status.
   */
  function setStatus(newStatus) {
    status.value = newStatus
  }

  /**
   * Sets the initialization status for a project.
   *
   * @param {Object} payload
   * @param {string} payload.project - The project identifier.
   * @param {boolean} payload.initialized - The initialization status.
   */
  function setProjectInit({ project, initialized }) {
    initializedProjects.value[project] = initialized
  }

  /**
   * Checks if a project has been initialized.
   *
   * @param {string} project - The project identifier.
   * @returns {boolean} - True if the project is initialized, false otherwise.
   */
  function isProjectInitialized(project) {
    return !!initializedProjects.value[project]
  }

  /**
   * Refreshes the import tasks for the current project.
   *
   * This action checks that the Neo4j application is running and that the
   * current project has been initialized. It then fetches import tasks from the server.
   *
   * @async
   * @returns {Promise<void>}
   */
  async function refreshSyncTasks() {
    const project = route.params.name
    if (status.value === AppStatus.Running && initializedProjects.value[project]) {
      const url = `/api/neo4j/full-imports?project=${project}`
      const response = await neo4jCore.request(url, { method: 'GET' })
      const tasks = await response.json()
      setImportTasks(tasks)
    }
  }

  /**
   * Refreshes the Neo4j application status.
   *
   * If the server reports that Neo4j is running, this action updates the status and retrieves the dump limit.
   * If not, it updates the status to stopped.
   *
   * @async
   * @returns {Promise<void>}
   */
  async function refreshStatus() {
    const res = await neo4jCore.request('/api/neo4j/status', { method: 'GET' })
    const { isRunning } = await res.json()
    if (isRunning) {
      setStatus(AppStatus.Running)
      const url = '/api/neo4j/graphs/dump/node-limit'
      const resLimit = await neo4jCore.request(url, { method: 'GET' })
      const dumpLimitText = await resLimit.text()
      setDumpLimit(parseInt(dumpLimitText))
    } else if (status.value === AppStatus.Running) {
      setStatus(AppStatus.Stopped)
    }
  }

  /**
   * Refreshes the graph counts for a specified project.
   *
   * This action fetches the current graph counts from the server if the Neo4j application is running
   * and the given project has been initialized.
   *
   * @async
   * @param {string} project - The project identifier.
   * @returns {Promise<void>}
   */
  async function refreshGraphCounts(project) {
    if (status.value === AppStatus.Running && initializedProjects.value[project]) {
      const url = `/api/neo4j/graphs/counts?project=${project}`
      const res = await neo4jCore.request(url, { method: 'GET' })
      const projectCounts = await res.json()
      setGraphCounts({ project, counts: projectCounts })
    }
  }

  return {
    // Getters
    isRunning,
    pendingImportTasks,
    // State
    status,
    dumpLimit,
    initializedProjects,
    syncTasks,
    graphCounts,
    // Actions
    isTaskPending,
    setDumpLimit,
    setImportTasks,
    setGraphCounts,
    setStatus,
    setProjectInit,
    isProjectInitialized,
    refreshSyncTasks,
    refreshStatus,
    refreshGraphCounts
  }
})

export default useNeo4jStore
