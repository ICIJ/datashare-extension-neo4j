<template>
  <div class="project-view-insights">
    <div class="container project-view-insights__container">
      <div class="project-view-insights__container__widget" :class="{ card: widget.card }">
        <!-- <vc-date-picker :key="`date-neo4j-dump`" v-model="selectedDateRange" is-range color="gray" class="border-0"
          :max-date="new Date()" :model-config="modelConfig" :locale="locale">
        </vc-date-picker> -->
        <b-container fluid>
          <b-row>
            <b-col md="10">
              <b-form @submit.prevent="dumpGraph" @reset="clearConditions">
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
              </b-form>
            </b-col>
            <b-col md="2" align-self="end">
              <b-button type="submit">Dump graph</b-button>
            </b-col>
          </b-row>
        </b-container>
      </div>
    </div>
  </div>
</template>


<script>

import { default as Condition } from '../store/Neo4jWhereCondition'
import { default as Neo4jWhereCondition } from '../components/Neo4jWhereCondition.vue'
// TODO: do we really neeed this ????


// TODO
// use bootstrap columns and row
// query builder
// create sub components for
// attribute adding 

// DOC_COLUMNS = {
//     DOC_ID: {NEO4J_CSV_COL: DOC_ID_CSV},
//     DOC_DIRNAME: {},
//     DOC_CONTENT_TYPE: {},
//     DOC_CONTENT_LENGTH: {NEO4J_CSV_COL: "LONG"},
//     DOC_EXTRACTION_DATE: {NEO4J_CSV_COL: "DATETIME"},
//     DOC_PATH: {},
// }
const DOC_CONTENT_TYPE = "contentType"
const DOC_PATH = "path"
const DOC_PROPERTIES = Object.fromEntries([
  [DOC_PATH, { type: "string" }],
  [DOC_CONTENT_TYPE, { type: "string" }]
])

export default {
  name: 'WidgetNeo4jDump',
  props: {
    widget: {
      type: Object
    }
  },
  components: {
    Neo4jWhereCondition
  },
  data() {
    // const schemaUnwrapped = Object.fromEntries(
    //   this.getDBSchema()
    //     .map(attr => [attr["property"], { value: null, conditionName: null, x: null }])
    // )

    return {
      conditions: [],
      dumpFormat: null,
      availableFormats: [
        { value: null, text: 'Select a format' },
        { value: 'cypher-shell', text: 'Cypher shell' },
        { value: 'graphml', text: 'GraphML' },
      ],
      // conditionOptions: {
      //   string: ["isEqualTo", "startsWith", "endsWith"]
      // }
    }

    // return {
    //   selectedDateRange: null,
    //   // selectedDateRange: {
    //   //   start: Date.now(),
    //   //   end: Date.now,
    //   // },
    //   // TODO: get this from the scheme
    //   ...schemaUnwrapped
    // }
  },
  computed: {
    locale() {
      return this.$i18n.locale
    },
    docFields() {
      return this.getDBSchema()
    },
    project() {
      return this.$store.state.search.index
    }
    // selectedDateRange: {
    //   get() {
    //     if (this.date?.start && this.date?.end) {
    //       const start = this.startTimeAdjust(this.date?.start)
    //       const end = this.endTimeAdjust(this.date?.end)
    //       return { start, end }
    //     }
    //     return this.date
    //   },
    //   set(values) {
    //     this.$emit('update', values)
    //   }
    // }
  },
  methods: {
    // startTimeAdjust(start) {
    //   return moment(start).locale(this.$i18n.locale).startOf('day').valueOf()
    // },
    // endTimeAdjust(end) {
    //   return moment(end).locale(this.$i18n.locale).endOf('day').valueOf()
    // },
    addCondition() {
      this.conditions.push(new Condition({ properties: DOC_PROPERTIES, variableName: 'doc' }))
    },
    async dumpGraph() {
      const request = this.getDumpRequest()
      var blob = this.postDumpRequest(request)
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
      return await this.sendActionAsRaw(`/graphs/dump?project=${this.project}`, config)
    },
    async sendActionAsRaw(url, config = {}) {
      const fullUrl = this.getFullUrl(`/api/neo4j${url}`)
      try {
        const r = await fetch(fullUrl, {
          body: JSON.stringify(config.data),
          ...config
        });
        if (r.ok) {
          return r
        }
        throw new Error(await r.text())
      } catch (error) {
        this.$core.api.eventBus?.$emit('http::error', error)
        throw error
      }
    },
    // This is static in the Api, we can't access it through this.$core.api
    getFullUrl(path) {
      // TODO: fix this....
      // const base = process.env.VUE_APP_DS_HOST || `${window.location.protocol}//${window.location.host}`
      const base = `${window.location.protocol}//${window.location.host}`
      const url = new URL(path, base)
      return url.href
    }
  }
}
</script>

<style lang="scss" scoped>
.widget--text {
  min-height: 100%;
}
</style>
