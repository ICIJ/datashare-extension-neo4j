<template>
  <div class="project-view-insights">
    <div class="container project-view-insights__container">
      <div class="project-view-insights__container__widget" :class="{ card: widget.card }">
        <b-container fluid>
          <b-row>
            <neo4j-status-badge :status="neo4jAppStatus"></neo4j-status-badge>
          </b-row>
          <b-form @submit.prevent="dumpGraph" @reset="clearConditions">
            <b-row>
              <b-col md="10">
                <b-row>
                  <b-col md="auto">
                    <label :for="`dump-format`">Dump format:</label>
                  </b-col>
                  <b-col md="4">
                    <b-form-select v-model="dumpFormat" :options="availableFormats" id="dump-format" required>Dump
                      format</b-form-select>
                  </b-col>
                </b-row>
                <ul>
                  <neo4j-where-condition v-for="(cond, index) in conditions" :condition=cond :key="cond.name"
                    v-on:removeCondition="conditions.splice(index, 1)"></neo4j-where-condition>
                </ul>
                <b-row align-h="start">
                  <b-col md="auto" class="pb-2">
                    <b-button v-on:click="addCondition">Add a filter</b-button>
                  </b-col>
                  <b-col md="auto" class="pb-2">
                    <b-button type="reset">Reset</b-button>
                  </b-col>
                </b-row>
              </b-col>
              <b-col md="2" align-self="end">
                <span id="disabled-wrapper" class="d-inline-block" tabindex="0">
                  <b-button type="submit" :disabled="!neo4jAppIsRunning">Dump graph</b-button>
                </span>
                <b-tooltip target="disabled-wrapper" v-if="dumpButtonToolTip !== null">{{ dumpButtonToolTip }}</b-tooltip>
              </b-col>
            </b-row>
          </b-form>
        </b-container>
      </div>
    </div>
  </div>
</template>


<script>
import { random } from 'lodash'
import { default as Condition } from '../core/Neo4jWhereCondition.js'
import { neo4jModule, AppStatus } from '../store/Neo4jModule'
import { default as Utils } from '../core/Utils'
import { default as Neo4jWhereCondition } from '../components/Neo4jWhereCondition.vue'
import { default as Neo4jStatusBadge } from '../components/Neo4jStatusBadge.vue'
import { default as polling } from '../core/mixin/polling'

const DOC_CONTENT_TYPE = "contentType"
const DOC_PATH = "path"
const DOC_PROPERTIES = Object.fromEntries([
  [DOC_PATH, { type: "string" }],
  [DOC_CONTENT_TYPE, { type: "string" }]
])
// TODO: can we do simpler than this
const SHOULD_START_APP_STATUSES = new Set([AppStatus.Error, AppStatus.Stopped].map(x => AppStatus[x]))

export default {
  name: 'WidgetNeo4jDump',
  props: {
    widget: {
      type: Object
    }
  },
  mixins: [polling],
  components: {
    Neo4jWhereCondition,
    Neo4jStatusBadge,
  },
  data() {
    return {
      conditions: [],
      dumpFormat: null,
      availableFormats: [
        { value: null, text: 'Select a format' },
        { value: 'cypher-shell', text: 'Cypher shell' },
        { value: 'graphml', text: 'GraphML' },
      ],
    }
  },
  async created() {
    // TODO: this should not be here, other components might use the state...
    const binded = this.$store.hasModule('neo4j')
    if (!binded) {
      this.$store.registerModule('neo4j', neo4jModule)
    }
    await this.resfreshNeo4jAppStatus()
    // TODO: probably same here
    await this.startNeo4jAppIfNeed()
    // TODO: probably same here
    const fn = this.resfreshNeo4jAppStatus
    const timeout = () => random(5000, 10000)
    this.registerPollOnce({ fn, timeout })
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
    project() {
      return this.$store.state.search.index
    },
    neo4jAppStatus() {
      return this.$store.getters['neo4j/status']
    },
    neo4jAppIsRunning() {
      return this.neo4jAppStatus === AppStatus.Running
    },
  },
  methods: {
    addCondition() {
      this.conditions.push(new Condition({ properties: DOC_PROPERTIES, variableName: 'doc' }))
    },
    async dumpGraph() {
      const request = this.getDumpRequest()
      let blob = this.postDumpRequest(request)
      if (blob) {
        // TODO: is this right ????
        const url = window.URL.createObjectURL(new Blob([blob]));
        const a = document.createElement('a');
        a.style.display = 'none';
        a.href = url;
        a.download = 'neo4j.dump';
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
      } else {
        // TODO: handle error
      }
    },
    clearConditions() {
      this.conditions = []
    },
    getDBSchema() {
      // TODO: actually get the schema
      return [
        {
          property: "dirname",
          type: "text",
          label: "Directory",
          placeholder: "Ex: /some/directory"
        },
        {
          property: "contentType",
          type: "text",
          label: "Content type",
          placeholder: "comma separated list of extension: pdf,png"
        },
        {
          property: "maxExtractionDate",
          type: "date",
          label: "Content type",
          placeholder: "comma separated list of extension: pdf,png"
        }
      ]
    },
    getDumpRequest() {
      return {
        format: this.dumpFormat,
        query: this.getGumpQuery(),
      }
    },
    getGumpQuery() {
      var where = this.conditions.map(c => c.toWhere()).filter(w => w !== null)
      if (!where.length) {
        return {}
      }
      if (where.length > 1) {
        where = { and: where }
      }
      return { where: where }
    },
    async postDumpRequest(request) {
      const config = {
        method: 'POST',
        data: request,
        headers: {
          "Content-Type": "application/json",
        },
      }
      return await Utils.request(`/graphs/dump?project=${this.project}`, config)
    },
    // TODO: move this elsewhere this shouldn't be handled here
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
      await Utils.request(`/start`, config)
      this.$store.commit('neo4j/status', AppStatus.Started)
    },
    // TODO: we might want to use the mapActions here
    async resfreshNeo4jAppStatus() {
      await this.$store.dispatch('neo4j/refreshStatus')
    }
  }
}
</script>

<style lang="scss" scoped>
.widget--text {
  min-height: 100%;
}
</style>
