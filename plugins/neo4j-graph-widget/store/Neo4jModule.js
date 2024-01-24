export const AppStatus = {
  Error: 'Error',
  Running: 'Running',
  Starting: 'Starting',
  Stopped: 'Stopped',
}

function actionBuilder(extension) {
  return {
    async refreshImportTasks({ commit, state }) {
      const project = this.state.insights.project
      if (state.neo4jAppStatus === AppStatus.Running && state.neo4jInitializedProjects[project]) {
        const project = this.state.insights.project
        const tasks = await extension.request(`/api/neo4j/full-imports?project=${project}`, { method: 'GET' })
        commit('importTasks', await tasks.json())
      }
    },
    async refreshStatus({ commit, state }) {
      const res = await extension.request('/api/neo4j/status', { method: 'GET' })
      const { isRunning } = await res.json()
      if (isRunning) {
        if (state.neo4jAppStatus !== AppStatus.Running) {
          commit('status', AppStatus.Running)
          const res = await extension.request('/api/neo4j/graphs/dump/node-limit', { method: 'GET' })
          const dumpLimit = await res.text()
          commit('dumpLimit', parseInt(dumpLimit))
        }
      } else if (state.neo4jAppStatus === AppStatus.Running) {
        commit('status', AppStatus.Stopped)
      }
    },
    async refreshGraphCounts({ commit, state }, project) {
      if (state.neo4jAppStatus === AppStatus.Running && state.neo4jInitializedProjects[project]) {
        const res = await extension.request(`/api/neo4j/graphs/counts?project=${project}`, { method: 'GET' })
        const projectCounts = await res.json()
        commit('graphCounts', { project: project, counts: projectCounts })
      }
    },
  }
}

function initialState() {
  return {
    neo4jAppStatus: AppStatus.Stopped,
    neo4jDumpLimit: null,
    neo4jInitializedProjects: {},
    neo4jImportTasks: null,
    neo4jGraphCounts: {},
  }
}

const state = initialState()
const mutations = {
  dumpLimit(state, newDumpLimit) {
    state.neo4jDumpLimit = newDumpLimit
  },
  importTasks(state, tasks) {
    window.datashare.localVue.set(state, 'neo4jImportTasks', tasks)
  },
  graphCounts(state, projectCounts) {
    window.datashare.localVue.set(state.neo4jGraphCounts, projectCounts.project, projectCounts.counts)
  },
  status(state, newStatus) {
    state.neo4jAppStatus = newStatus
  },
  projectInit(state, projectState) {
    window.datashare.localVue.set(state.neo4jInitializedProjects, projectState.project, projectState.initialized)
  },
}
const getters = {
  dumpLimit(state) {
    return state.neo4jDumpLimit
  },
  importTasks(state) {
    return state.neo4jImportTasks
  },
  graphCounts(state) {
    return state.neo4jGraphCounts
  },
  status(state) {
    return state.neo4jAppStatus
  },
  projectsInit(state) {
    return state.neo4jInitializedProjects
  },
}

function neo4jStoreBuilder(extension) {
  return {
    namespaced: true,
    state,
    mutations,
    getters,
    actions: actionBuilder(extension)
  }
}


export default neo4jStoreBuilder
