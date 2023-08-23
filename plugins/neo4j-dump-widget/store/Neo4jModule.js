export const AppStatus = {
  Error: 'Error',
  Running: 'Running',
  Starting: 'Starting',
  Stopped: 'Stopped',
}

function actionBuilder(extension) {
  return {
    async refreshStatus({ commit, state }) {
      const res = await extension.request('/api/neo4j/status', { method: 'GET' })
      const { isRunning } = await res.json()
      if (isRunning) {
        if (state.neo4jAppStatus !== AppStatus.Running) {
          commit('status', AppStatus.Running)
        }
      } else if (state.neo4jAppStatus === AppStatus.Running) {
        commit('status', AppStatus.Stopped)
      }
    }
  }
}

function initialState() { return { neo4jAppStatus: AppStatus.Stopped } }

const state = initialState()
const mutations = {
  status(state, newStatus) {
    state.neo4jAppStatus = newStatus
  }
}
const getters = {
  status(state) {
    return state.neo4jAppStatus
  }
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
