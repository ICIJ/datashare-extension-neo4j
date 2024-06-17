<template>
  <div class="d-flex flex-column py-2">
    <template v-if="hasTasks">
      <h5>Import tasks</h5>
      <div class="col">
        <!-- TODO: use sass here rather than style -->
        <div style="height: 100px;overflow-y: scroll;" class="card">
          <div v-for="t in this.neo4jImportTasks" :key=t.id class="d-flex p-2">
            <div>
              <ellipse-status :status="t.status" :progress="t.progress" horizontal />
            </div>
            <div class="small">
              {{ displayTaskDate(t) }}
            </div>
          </div>
        </div>
      </div>
    </template>
    <div class="col d-flex flex-row align-items-center pt-2" :class="{ 'justify-content-center': !hasTasks }">
      <div v-if="!isServer" class="me-2">
        <b-form @submit.prevent="importGraph">
          <span id="disabled-import-wrapper">
            <b-button
              v-if="neo4jImportTasks ?? false"
              type="submit"
              :disabled="!neo4jAppIsRunning"
              variant="primary">
              {{ importButton }}
            </b-button>
          </span>
          <b-tooltip target="disabled-import-wrapper">{{ importButtonToolTip }}</b-tooltip>
        </b-form>
      </div>
      <span v-if="latestDone" class="small">
        Last updated on {{ localeDate(latestDone.completedAt) }}, {{ localeTime(latestDone.completedAt) }}
      </span>
    </div>
  </div>
</template>

<script>
import random from 'lodash/random'
import { mapState } from 'vuex'
import { AppStatus } from '../store/Neo4jModule'
import { default as polling } from '../core/mixin/polling'
import { humanShortDate, humanTime } from '../utils/humanDate'
// TODO: this should be imported from the client rather than duplicated
import EllipseStatus from '../components/EllipseStatus.vue'


export const TaskStatus = {
  Created: "CREATED",
  Queued: "QUEUED",
  Running: "RUNNING",
  Retry: "RETRY",
  Error: "ERROR",
  Done: "DONE",
  Cancelled: "CANCELLED",
}

const TASK_READY_STATES = Object.freeze(new Set([TaskStatus.Done, TaskStatus.Error, TaskStatus.Cancelled]))

const SERVER_MODE = "server"

export default {
  name: 'Neo4jImport',
  data() {
    return {
      registeredPolls: []
    }
  },
  components: {
    EllipseStatus,
  },
  mixins: [polling],
  computed: {
    hasTasks() {
      return !!this.neo4jImportTasks?.length
    },
    importButton() {
      const projectHasDocuments = this.neo4jGraphCounts[this.project]?.documents
      return this.hasTasks || projectHasDocuments ? 'Update graph' : 'Create graph'
    },
    importButtonToolTip() {
      if (this.neo4jAppStatus === AppStatus.Starting) {
        return 'neo4j extension is starting...'
      }
      if (!this.neo4jAppIsRunning) {
        return 'neo4j extension is not running, refresh this page to start it or wait'
      }
      var tooltip = 'Graph import can be resource intensive, use it with care.'
      if (this.latestDone) {
        tooltip += `
Note that updating the graph will only add new documents and entities and update modified ones, it will not delete data.`
      }
      return tooltip
    },
    isServer() {
      return this.$core.mode.modeName === SERVER_MODE
    },
    latestDone() {
      return this.neo4jImportTasks?.find(t => t.status === TaskStatus.Done) ?? null
    },
    neo4jAppIsRunning() {
      return this.neo4jAppStatus === AppStatus.Running
    },
    project() {
      return this.$store.state.insights.project
    },
    projectReady() {
      return this.neo4jAppIsRunning && this.neo4jInitializedProjects[this.project]
    },
    runningTasks() {
      return this.neo4jImportTasks?.filter((t) => !TASK_READY_STATES.has(t.status)) ?? []
    },
    ...mapState('neo4j', ['neo4jAppStatus', 'neo4jImportTasks', 'neo4jInitializedProjects', 'neo4jGraphCounts'])
  },
  async mounted() {
    const longTimeout = () => random(5000, 10000)
    const refreshTasks = this.refreshImportTasks
    this.registerPoll({ fn: refreshTasks, timeout: longTimeout, immediate: true })
  },
  unmounted() {
    this.unregisteredPolls()
  },
  watch: {
    async projectReady() {
      await this.$store.dispatch('neo4j/refreshImportTasks')
    },
    async project() {
      await this.$store.dispatch('neo4j/refreshImportTasks')
    }
  },
  methods: {
    displayTaskDate(task) {
      const date = task.status === TaskStatus.Running ? task.createdAt : task.completedAt ?? task.createdAt
      return `${this.localeDate(date)}, ${this.localeTime(date)}`
    },
    localeDate(date) {
      return humanShortDate(date, this.$i18n.locale)
    },
    localeTime(date) {
      return humanTime(date, this.$i18n.locale)
    },
    async importGraph() {
      const config = { method: 'POST', headers: { "Content-Type": "application/json" } }
      await this.$neo4jCore.request(`/api/neo4j/full-imports?project=${this.project}`, config)
      return this.$store.dispatch('neo4j/refreshImportTasks')
    },
    async refreshImportTasks() {
      await this.$store.dispatch('neo4j/refreshImportTasks')
      return true
    },
  },
}
</script>
