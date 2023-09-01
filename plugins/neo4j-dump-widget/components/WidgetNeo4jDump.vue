<template>
  <div>
    <div class="widget__header align-items-center card-header d-md-flex">
      <h4 class="m-0 mr-2">Graph export</h4>
      <neo4j-status-badge :status="neo4jAppStatus"></neo4j-status-badge>
    </div>
    <div class="project-view-insights card-body">
      <b-form flex-column @submit.prevent="dumpGraph" @reset="clear">
        <div class="row">
          <div class="col-12 col-md-6 flex-column">
            <b-form-group>
              <label for="dump-format" class="col-md-4 col-form-label">Export format</label>
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
          <div class="col-12 col-md-6 flex-column d-flex justify-content-end align-items-end">
            <div>
              <b-button type="reset" variant="danger" class="mr-2">Reset</b-button>
              <span id="disabled-wrapper">
                <b-button type="submit" :disabled="!neo4jAppIsRunning" variant="primary">Export graph</b-button>
              </span>
              <b-tooltip target="disabled-wrapper" v-if="dumpButtonToolTip !== null">{{ dumpButtonToolTip }}</b-tooltip>
            </div>
          </div>
        </div>
      </b-form>
    </div>
  </div>
</template>


<script>
import bodybuilder from 'bodybuilder'
import { concat, get, map, random } from 'lodash'
import { mapState } from 'vuex'
import { default as Condition } from '../core/Neo4jWhereCondition.js'
import { AppStatus } from '../store/Neo4jModule'
import { default as Neo4jStatusBadge } from '../components/Neo4jStatusBadge.vue'
import { default as polling } from '../core/mixin/polling'


const DOC_CONTENT_TYPE = "contentType"
const DOC_PATH = "path"
const DOC_PROPERTIES = Object.fromEntries([
  [DOC_PATH, { type: "localPath" }],
  [DOC_CONTENT_TYPE, { type: "string" }]
])
const SHOULD_START_APP_STATUSES = new Set([AppStatus.Error, AppStatus.Stopped].map(x => AppStatus[x]))
const GRAPHML = 'graphml'

export default {
  name: 'WidgetNeo4jDump',
  props: {
    widget: {
      type: Object
    }
  },
  mixins: [polling],
  components: {
    Neo4jStatusBadge,
  },
  data() {
    return {
      availableFormats: [
        { value: null, text: 'Select a format' },
        { value: 'cypher-shell', text: 'Cypher shell' },
        { value: GRAPHML, text: 'GraphML' },
      ],
      dumpFormat: null,
      fileType: null,
      fileTypes: [],
      selectedFileTypes: [],
      selectedPath: this.$config.get('mountedDataDir') || this.$config.get('dataDir'),
    }
  },
  async created() {
    this.getFileTypes()
    await this.resfreshNeo4jAppStatus()
    await this.startNeo4jAppIfNeed()
    const fn = this.resfreshNeo4jAppStatus
    const timeout = () => random(5000, 10000)
    this.registerPollOnce({ fn, timeout })
  },
  unmounted() {
    this.unregisteredPolls()
  },
  computed: {
    locale() {
      return this.$i18n.locale
    },
    docFields() {
      return this.getDBSchema()
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
    project() {
      return this.$store.state.search.index
    },
    neo4jAppIsRunning() {
      return this.neo4jAppStatus === AppStatus.Running
    },
    ...mapState('neo4j', ['neo4jAppStatus'])
  },
  methods: {
    addCondition() {
      this.conditions.push(new Condition({ properties: DOC_PROPERTIES, variableName: 'doc' }))
    },
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
    async dumpGraph() {
      const request = this.getDumpRequest()
      const res = await this.postDumpRequest(request)
      const blob = await res.blob()
      const url = window.URL.createObjectURL(new Blob([blob]));
      const a = document.createElement('a');
      a.style.display = 'none';
      a.href = url;
      a.download = `datashare-graph${this.dumpExtension}`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
    },
    clear() {
      this.dumpFormat = null
      this.selectedPath = this.$config.get('mountedDataDir') || this.$config.get('dataDir')
      this.selectedFileTypes = []
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
    getDBSchema() {
      return [
        {
          property: 'dirname',
          type: 'text',
          label: 'Directory',
          placeholder: 'Ex: /some/directory'
        },
        {
          property: 'contentType',
          type: 'text',
          label: 'Content type',
          placeholder: 'comma separated list of extension: pdf,png'
        },
        {
          property: 'maxExtractionDate',
          type: 'date',
          label: 'Content type',
          placeholder: 'comma separated list of extension: pdf,png'
        }
      ]
    },
    getDumpRequest() {
      return {
        format: this.dumpFormat,
        query: this.getDumpQuery(),
      }
    },
    getDumpQuery() {
      let where = []
      if (this.selectedFileTypes) {
        where = this.selectedFileTypes.map(t => this.fileTypeToWhere(t))
      }
      if (this.selectedPath) {
        where.push(this.filePathToWhere(this.selectedPath))
      }
      if (where) {
        if (where.length > 1) {
          where = { and: where }
        } else {
          where = where[0]
        }
        return { where: null }
      }
      return {}
    },
    postDumpRequest(request) {
      const config = {
        method: 'POST',
        data: request,
        headers: {
          "Content-Type": "application/json",
        },
        responseType: 'stream'
      }
      return this.$neo4jCore.request(`/api/neo4j/graphs/dump?project=${this.project}`, config)
    },
    async getFileTypes() {
      this.$wait.start('load all file types')
      this.fileTypes = await this.aggregate('contentType', 'contentType')
      this.$wait.end('load all file types')
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
      this.$store.commit('neo4j/status', AppStatus.Started)
    },
    async resfreshNeo4jAppStatus() {
      await this.$store.dispatch('neo4j/refreshStatus')
    },
    async treeView() {
      return this.$core.findComponent('TreeView')
    },
  }
}
</script>

<style lang="scss" scoped></style>
