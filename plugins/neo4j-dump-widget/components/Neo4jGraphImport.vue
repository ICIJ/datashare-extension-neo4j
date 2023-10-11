<template>
  <div>
    <div v-if="runningTasks.length > 0" class="row">
      {{ runningTasks.length }} running imports
    </div>
    <div v-if="runningImportMetadata !== null">
      Latest import:
      <ellipse-status :status="runningImportMetadata.status" :progress="runningImportMetadata.progress" horizontal />
    </div>
    <div v-if="!isServer" class="row">
      <b-form flex-column @submit.prevent="importGraph">
        <span id="disabled-import-wrapper">
          <b-button type="submit" :disabled="!neo4jAppIsRunning" variant="primary">Update graph</b-button>
        </span>
        <b-tooltip target="disabled-import-wrapper">{{ importButtonToolTip }}</b-tooltip>
      </b-form>
    </div>
  </div>
</template>

<script>
import { random } from 'lodash'
import { mapState } from 'vuex'
import { AppStatus } from '../store/Neo4jModule'
import { default as polling } from '../core/mixin/polling'
// TODO: this should be imported from the client rather than duplicated
import EllipseStatus from '../components/EllipseStatus'


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

export default {
  name: 'Neo4jImport',
  data() {
    return {
      runningImportMetadata: null,
      runningImportMetadataPoll: null,
    }
  },
  components: {
    EllipseStatus,
  },
  mixins: [polling],
  computed: {
    importButtonToolTip() {
      if (this.neo4jAppStatus === AppStatus.Starting) {
        return 'neo4j extension is starting...'
      }
      if (!this.neo4jAppIsRunning) {
        return 'neo4j extension is not running, refresh this page to start it or wait'
      }
      return 'Graph import can be resource intensive, use it with care.'
    },
    isServer() {
      return this.$core.mode === 'SERVER'
    },
    project() {
      return this.$store.state.insights.project
    },
    neo4jAppIsRunning() {
      return this.neo4jAppStatus === AppStatus.Running
    },
    projectReady() {
      return this.neo4jAppIsRunning && this.neo4jInitializedProjects[this.project]
    },
    runningTasks() {
      return this.neo4jImportTasks?.filter((t) => !TASK_READY_STATES.has(t.status)) ?? []
    },
    ...mapState('neo4j', ['neo4jAppStatus', 'neo4jImportTasks', 'neo4jRunningImport', 'neo4jInitializedProjects'])
  },
  async created() {
    const longTimeout = () => random(5000, 10000)
    const refreshTasks = this.refreshImportTasks
    this.registerPoll({ fn: refreshTasks, timeout: longTimeout, immediate: true })
  },
  unmounted() {
    this.unregisteredPolls()
  },
  watch: {
    async projectReady() {
      await this.refreshRunningImportMetadata()
      await this.$store.dispatch('neo4j/refreshRunningImport')
      await this.$store.dispatch('neo4j/refreshImportTasks')
    },
    async project() {
      await this.refreshRunningImportMetadata()
      await this.$store.dispatch('neo4j/refreshRunningImport')
      await this.$store.dispatch('neo4j/refreshImportTasks')
    },
    async neo4jRunningImport(newImport, oldImport) {
      if (newImport !== null && newImport !== oldImport) {
        if (this.runningImportMetadataPoll) {
          this.unregisteredPoll({ id: this.runningImportMetadataPoll })
        }
        const shortTimeout = () => random(2000, 4000)
        const refreshRunningTask = this.refreshRunningImportMetadata
        this.runningImportMetadataPoll = this.registerPoll({ fn: refreshRunningTask, timeout: shortTimeout, immediate: true })
        await this.$store.dispatch('neo4j/refreshImportTasks')
      }
    }
  },
  methods: {
    async importGraph() {
      const config = { method: 'POST', headers: { "Content-Type": "application/json" } }
      const res = await this.$neo4jCore.request(`/api/neo4j/full-imports?project=${this.project}`, config)
      this.$store.commit('neo4j/runningImport', await res.text())
      return this.$store.dispatch('neo4j/refreshImportTasks', this.project)
    },
    async refreshImportTasks() {
      await this.$store.dispatch('neo4j/refreshImportTasks', this.project)
      return true
    },
    async refreshRunningImportMetadata() {
      if (this.projectReady && this.neo4jRunningImport !== null) {
        const newImport = this.runningImportMetadata?.id !== this.neo4jRunningImport;
        const trackProgress = !TASK_READY_STATES.has(this.runningImportMetadata?.status);
        if (newImport || trackProgress) {
          const res = await this.$neo4jCore.request(
            `/api/neo4j/tasks/${this.neo4jRunningImport}?project=${this.project}`,
            { method: 'GET' }
          )
          this.runningImportMetadata = await res.json()
          if (TASK_READY_STATES.has(this.runningImportMetadata.status)) {
            await this.$store.dispatch('neo4j/refreshGraphCounts')
          }
        }
      }
      return true
    }
  },
}
</script>

<style lang="scss" scoped>
.widget {
  min-height: 100%;
  position: relative;

  &__spinner {
    text-align: center;
    width: 100%;
    // TODO: put this back (import bootstrap ???)
    // padding: $spacer;
  }

  &__content {
    &__count {
      &--muted {
        // TODO: put this back (import bootstrap ???)
        // color: $text-muted;
      }
    }
  }
}
</style>
