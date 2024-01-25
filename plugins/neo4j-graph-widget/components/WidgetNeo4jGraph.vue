<template>
  <div>
    <div class="widget__header align-items-center card-header d-md-flex">
      <h4 class="m-0 mr-2">neo4j</h4>
      <neo4j-status-badge :status="neo4jAppStatus"></neo4j-status-badge>
    </div>
    <b-form flex-column @submit.prevent="displayConfirmOverlay" @reset="clear">
      <div class="project-view-insights card-body pb-0">
        <div class="row">
          <div class="d-flex flex-column col-12 col-md-6 justify-content-between">
            <neo4j-graph-count></neo4j-graph-count>
            <neo4j-graph-import></neo4j-graph-import>
          </div>
          <div class="col-12 col-md-6 flex-column">
            <h5 class="col-md-4">Export</h5>
            <b-form-group>
              <label for="dump-format" class="col-md-4 col-form-label">Format</label>
              <b-form-select
                class="col-md-8"
                v-model="dumpFormat"
                :options="availableFormats"
                id="dump-format"
                required>Export format
              </b-form-select>
            </b-form-group>
            <b-form-group>
              <label for="file-types" class="col-md-4 col-form-label">File types</label>
              <b-overlay :show="$wait.is('load all file types')" opacity="0.6" rounded spinner-small no-wrap>
              </b-overlay>
              <b-form-select
                class="col-md-8"
                v-model="selectedFileTypes"
                multiple
                :options="fileTypes"
                id="file-types">
              </b-form-select>
            </b-form-group>
            <b-form-group>
              <div class="d-flex flex-nowrap g-0">
                <label for="input-selected-path" class="col-md-4 col-form-label">Project directory</label>
                <div class="col-md-8 px-0">
                  <b-input-group>
                    <b-form-input :value="selectedPath"
                      id="input-selected-path"
                      type="text"
                      disabled></b-form-input>
                    <b-button variant="primary" v-b-modal="`treeview`" class="input-group-append">
                      Select path
                    </b-button>
                    <b-modal
                      id="treeview"
                      lazy scrollable
                      size="lg">
                      <component :is="treeView"
                        v-model="selectedPath"
                        id="treeview"
                        :projects="[project]"
                        selectable
                        count>
                      </component>
                    </b-modal>
                  </b-input-group>
                </div>
              </div>
            </b-form-group>
          </div>
        </div>
      </div>
      <div class="project-view-insights card-footer">
        <div class="row">
          <div class="col-12 d-flex justify-content-end align-items-end">
            <b-button type="reset" variant="danger" class="mr-2">Reset</b-button>
            <span id="disabled-wrapper">
              <b-button ref="submit" type="submit" :disabled="!neo4jAppIsRunning" variant="primary">Export
                graph</b-button>
            </span>
            <b-tooltip target="disabled-wrapper" v-if="dumpButtonToolTip !== null">{{ dumpButtonToolTip }}</b-tooltip>
          </div>
        </div>
        <b-overlay :show="showOverlay" no-wrap @shown="onShowingOverlay" @hidden="onHidingOverlay" opacity=0.95>
          <template #overlay>
            <div
              ref="dialog"
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
                <b-button variant="outline-primary" class="mr-3" @click="onCancellingExport">
                  Back
                </b-button>
                <b-button variant="primary" @click="onExportConfirmation">Export graph</b-button>
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
import { concat, get, map, random } from 'lodash'
import { mapState } from 'vuex'
import { AppStatus } from '../store/Neo4jModule'
import { default as Neo4jGraphCount } from '../components/Neo4jGraphCount.vue'
import { default as Neo4jStatusBadge } from '../components/Neo4jStatusBadge.vue'
import { default as Neo4jGraphImport } from '../components/Neo4jGraphImport.vue'
import { default as polling } from '../core/mixin/polling'

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
      type: Object
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
        { value: null, text: 'Select a format' },
        { value: CYPHER_SHELL, text: 'Cypher shell' },
        { value: GRAPHML, text: 'GraphML' },
      ],
      dumpFormat: null,
      dumpDocUrl: null,
      fileType: null,
      fileTypes: [],
      initializedProject: false,
      selectedFileTypes: [],
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
    neo4jAppIsRunning() {
      return this.neo4jAppStatus === AppStatus.Running
    },
    ...mapState('neo4j', ['neo4jAppStatus', 'neo4jDumpLimit'])
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
      this.$refs.form.submit();
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
      this.$refs.submit.focus()
    },
    onShowingOverlay() {
      this.$refs.dialog.focus()
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
    },
    async treeView() {
      return this.$core.findComponent('TreeView')
    },
  }
}
</script>

<style lang="scss" scoped></style>
