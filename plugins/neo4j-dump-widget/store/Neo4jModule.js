import { default as Utils } from '../core/Utils'

export const AppStatus = {
  Error: 'Error',
  Running: 'Running',
  Starting: 'Starting',
  Stopped: 'Stopped',
}

export const neo4jModule = {
  namespaced: true,
  state: () => ({ neo4jAppStatus: AppStatus.Stopped }),
  getters: {
    status(state) {
      return state.neo4jAppStatus
    }
  },
  mutations: {
    status(state, newStatus) {
      state.neo4jAppStatus = newStatus
    }
  },
  actions: {
    async refreshStatus({ commit, state }) {
      const res = await Utils.request('/status', { method: 'GET' })
      const {isRunning} = await res.json()
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

export default neo4jModule