<template>
  <div>
    <div class="widget__header align-items-center card-header d-md-flex">
      <h4 class="m-0 me-2">neo4j</h4>
      <neo4j-status-badge :status="neo4jAppStatus"></neo4j-status-badge>
    </div>
    <b-form class="d-flex flex-column" @submit.prevent="displayConfirmOverlay" @reset="clear">
      <div class="project-view-insights card-body pb-0">
        <div class="row">
          <div class="d-flex flex-column col-12 col-md-6 justify-content-start">
            <neo4j-graph-count></neo4j-graph-count>
            <neo4j-graph-import></neo4j-graph-import>
          </div>
          <div class="col-12 col-md-6 flex-column">
            <h5 class="col-md-4">Export</h5>
            <div class="row mb-3">
              <label for="dump-format" class="col-md-4 col-form-label">Format</label>
              <div class="col">
                <b-form-select
                  class="col-md-8"
                  v-model="dumpFormat"
                  :options="availableFormats"
                  id="dump-format"
                  required>
                  Export format
                </b-form-select>
              </div>
            </div>
            <div class="row mb-3">
              <label for="file-types" class="col-md-4 col-form-label">File types</label>
              <div class="col">
                <div class="position-relative">
                  <b-overlay :show="$wait.is('load all file types')" opacity="0.6" rounded spinner-small no-wrap>
                    <b-form-select
                    class="col-md-8"
                    v-model="selectedFileTypes"
                    multiple
                    :options="fileTypes"
                    id="file-types"></b-form-select>
                  </b-overlay>
                </div>
              </div>
            </div>
            <div class="row mb-3 flex-nowrap">
              <label for="input-selected-path" class="col-md-4 col-form-label">Project directory</label>
              <div class="col-md-8">
                <b-input-group>
                  <input 
                    :value="selectedPath"                      
                    class="form-control"
                    type="text"
                    disabled />
                  <b-button id="input-selected-path" variant="primary" v-b-modal="`treeview`" class="input-group-append" :disabled="!treeView">
                    Select path
                  </b-button>
                  <b-modal
                    id="treeview"
                    body-class="p-0 border-bottom"
                    cancel-variant="outline-primary"
                    :cancel-title="$t('global.cancel')"
                    hide-header
                    lazy
                    :ok-title="$t('widget.creationDate.selectFolder')"
                    scrollable
                    size="lg"
                    @ok="selectedPath = treeViewPath">
                    <component 
                      :is="treeView"
                      :path="treeViewPath || selectedPath"
                      @update:path="treeViewPath = $event"
                      :projects="[project]"
                      count
                      size>
                    </component>
                  </b-modal>
                </b-input-group>
              </div>
            </div>
          </div>
        </div>
      </div>
      <div class="project-view-insights card-footer">
        <div class="row">
          <div class="col-12 d-flex justify-content-end align-items-end">
            <b-button type="reset" variant="outline-primary" class="me-2">
              Reset
            </b-button>
            <span id="disabled-wrapper">
              <b-button ref="submit" type="submit" :disabled="!(neo4jAppIsRunning && projectDocs > 0)" variant="primary">
                Export graph
              </b-button>
            </span>
            <b-tooltip target="disabled-wrapper" v-if="dumpButtonToolTip !== null">
              {{ dumpButtonToolTip }}
            </b-tooltip>
          </div>
        </div>
        <b-overlay :show="showOverlay" no-wrap @hidden="onHidingOverlay" opacity=0.95>
          <template #overlay>
            <div
              tabindex="-1"
              role="dialog"
              aria-modal="false"
              class="p-3">
              <p>
                A maximum of {{ neo4jDumpLimit }} documents and their related named entities will be exported.
                <br>To export the entire graph, proceed to a <a :href="dumpDocUrl">full DB dump</a> or ask the system administrator for it.
              </p>
              <p>Download can take some time to start, <strong>please don't close the opened tab</strong> until then !</p>
              <div class="d-flex align-items-center justify-content-right">
                <b-button variant="outline-primary" class="me-3" @click="onCancellingExport">
                  Back
                </b-button>
                <b-button variant="primary" @click="onExportConfirmation">
                  Export graph
                </b-button>
              </div>
            </div>
          </template>
        </b-overlay>
      </div>
    </b-form>
    <b-form ref="form" method="POST" :action=dumpUrl target="_blank">
      <input type="hidden" name="dumpRequest" :value=dumpRequestJson />
    </b-form>
  </div>
</template>


<script>
import bodybuilder from 'bodybuilder'
import concat from 'lodash/concat'
import get from 'lodash/get'
import map from 'lodash/map'
import random from 'lodash/random'
import { defineAsyncComponent } from 'vue'

import { AppStatus } from '../store/Neo4jModule'
import Neo4jGraphCount from '../components/Neo4jGraphCount.vue'
import Neo4jStatusBadge from '../components/Neo4jStatusBadge.vue'
import Neo4jGraphImport from '../components/Neo4jGraphImport.vue'
import polling from '../core/mixin/polling'

const SHOULD_START_APP_STATUSES = new Set([AppStatus.Error, AppStatus.Stopped].map(x => AppStatus[x]))
const DUMP_DOC_URL = 'https://neo4j.com/docs/operations-manual/current/backup-restore/offline-backup/'
const CYPHER_SHELL = 'cypher-shell'
const GRAPHML = 'graphml'
const MATCH_DOC_NODE = {
  "path": {
    "nodes": [
      {
        "name": "doc",
        "labels": ["Document"]
      }
    ]
  }
}

export default {
  name: 'WidgetNeo4jGraph',
  props: {
    widget: {
      type: Object,
      default: () => {}
    }
  },
  mixins: [polling],
  components: {
    Neo4jGraphCount,
    Neo4jStatusBadge,
    Neo4jGraphImport,
  },
  data() {
    return {
      availableFormats: [
        { value: CYPHER_SHELL, text: 'Cypher shell' },
        { value: GRAPHML, text: 'GraphML' },
      ],
      dumpFormat: CYPHER_SHELL,
      dumpDocUrl: null,
      fileType: null,
      fileTypes: [],
      initializedProject: false,
      selectedFileTypes: [],
      treeViewPath: null,
      selectedPath: this.$config.get('mountedDataDir') || this.$config.get('dataDir'),
      showOverlay: null,
    }
  },
  async mounted() {
    this.getFileTypes()
    await this.resfreshNeo4jAppStatus()
    await this.startNeo4jAppIfNeed()
    this.dumpDocUrl = DUMP_DOC_URL
    const fn = this.resfreshNeo4jAppStatus
    const timeout = () => random(5000, 10000)
    this.registerPollOnce({ fn, timeout })
  },
  unmounted() {
    this.unregisteredPolls()
  },
  watch: {
    async neo4jAppIsRunning() {
      if (this.neo4jAppIsRunning && !this.initializedProject) {
        await this.initProject()
      }
    }
  },
  computed: {
    locale() {
      return this.$i18n.locale
    },
    dumpButtonToolTip() {
      if (this.neo4jAppStatus === AppStatus.Starting) {
        return "neo4j extension is starting..."
      } else if (!this.neo4jAppIsRunning) {
        return "neo4j extension is not running, refresh this page to start it or wait "
      } else if (!this.projectDocs > 0) {
        return "neo4j graph is empty, create it first to be able to export it !"
      }
      return null
    },
    dumpExtension() {
      if (this.dumpFormat === null) {
        return null
      }
      return this.dumpFormat === GRAPHML ? '.graphml' : '.dump'
    },
    dumpQuery() {
      let where = this.selectedFileTypes ? this.selectedFileTypes.map(this.fileTypeToWhere) : []
      if (this.selectedPath) {
        where.push(this.filePathToWhere(this.selectedPath))
      }
      if (where.length === 1) {
        return { queries: [{ matches: [MATCH_DOC_NODE], where: where[0] }] }
      } else if (where.length) {
        return { queries: [{ matches: [MATCH_DOC_NODE], where: { and: where } }] }
      }
      return {}
    },
    dumpRequest() {
      return {
        format: this.dumpFormat,
        query: this.dumpQuery,
      }
    },
    dumpRequestJson() {
      return JSON.stringify(this.dumpRequest)
    },
    dumpUrl() {
      return this.$neo4jCore.getFullUrl(`/api/neo4j/graphs/dump?project=${this.project}`)
    },
    project() {
      return this.$store.state.insights.project
    },
    projectDocs() {
      return this.neo4jGraphCounts[this.project]?.documents || 0
    },
    neo4jAppIsRunning() {
      return this.neo4jAppStatus === AppStatus.Running
    },
    treeView() {
      return defineAsyncComponent(() => this.$core.findComponent('TreeView'))
    },
    neo4jAppStatus() {
      return this.$store.state.neo4j.neo4jAppStatus
    },
    neo4jDumpLimit() {
      return this.$store.state.neo4j.neo4jDumpLimit
    },
    neo4jGraphCounts() {
      return this.$store.state.neo4j.neo4jGraphCounts
    }
  },
  methods: {
    async aggregate(field, name) {
      let body, options, responses, searchResult
      let after = null
      let result = []
      while (responses === undefined || responses.length === 10) {
        options = after ? { after } : {}
        body = bodybuilder()
          .size(0)
          .agg('composite', { sources: [{ field: { terms: { field } } }] }, options, name)
          .build()
        searchResult = await this.$core.api.elasticsearch.search({
          index: this.project,
          body
        })
        after = get(searchResult, ['aggregations', name, 'after_key'], null)
        responses = get(searchResult, ['aggregations', name, 'buckets'], [])
        result = concat(result, map(responses, 'key.field'))
      }
      return result
    },
    clear() {
      this.dumpFormat = null
      this.selectedPath = this.$config.get('mountedDataDir') || this.$config.get('dataDir')
      this.selectedFileTypes = []
    },
    async dumpGraph() {
      this.$refs.form?.$el?.submit()
    },
    displayConfirmOverlay() {
      this.showOverlay = true
    },
    fileTypeToWhere(type) {
      return {
        isEqualTo: {
          property: {
            variable: 'doc',
            name: 'contentType',
          },
          value: {
            literal: type,
          }
        }
      }
    },
    filePathToWhere(path) {
      return {
        startsWith: {
          property: {
            variable: 'doc',
            name: 'path',
          },
          value: {
            literal: path,
          }
        }
      }
    },
    async getFileTypes() {
      this.$wait.start('load all file types')
      this.fileTypes = await this.aggregate('contentType', 'contentType')
      this.$wait.end('load all file types')
    },
    async initProject() {
      await this.$neo4jCore.request(`/api/neo4j/init?project=${this.project}`, { method: 'POST' })
      this.$store.commit('neo4j/projectInit', { project: this.project, initialized: true })
    },
    onCancellingExport() {
      this.showOverlay = false
    },
    onExportConfirmation() {
      this.dumpGraph()
      this.showOverlay = false
    },
    onHidingOverlay() {
      this.$refs.submit?.$el?.focus()
    },
    async startNeo4jAppIfNeed() {
      const shouldStart = SHOULD_START_APP_STATUSES;
      if (shouldStart.has(this.$store.getters['neo4j/status'])) {
        this.startNeo4jApp()
      }
    },
    async startNeo4jApp() {
      const config = {
        method: 'POST',
        headers: {
          "Content-Type": "application/json",
        },
      }
      this.$store.commit('neo4j/status', AppStatus.Starting)
      await this.$neo4jCore.request('/api/neo4j/start', config)
      this.$store.commit('neo4j/status', AppStatus.Running)
    },
    async resfreshNeo4jAppStatus() {
      await this.$store.dispatch('neo4j/refreshStatus')
      return true
    }
  }
}
</script>

<style lang="scss" scoped></style>
