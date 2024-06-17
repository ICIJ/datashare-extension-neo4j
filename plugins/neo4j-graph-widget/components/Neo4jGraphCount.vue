<template>
  <v-wait :for="loader" transition="fade">
    <template #waiting>
      <div class="flex-grow text-center widget__spinner">
        <fa icon="circle-notch" spin size="2x" />
      </div>
    </template>
    <h5>Graph statistics</h5>
    <div class="widget__content">
      <div v-if="documents > 0" class="d-flex flex-row flex-wrap">
        <div class="col col-md-4 my-2">
          <div
            class="card py-2 bg-light widget__content__count align-items-center"
            data-toggle="tooltip"
            :title=documentTitle>
            <fa icon="file" class="widget__icon" />
            <span>
              {{ humanNumber(documents, $t('human.number')) }}
            </span>
          </div>
        </div>
        <div
          v-for="category in categories"
          :key="category"
          class="col col-md-4 my-2">
          <div data-toggle="tooltip"
            class="card py-2 bg-light widget__content__count align-items-center"
            :class="{ 'widget__content__count--muted': !namedEntities[category] }"
            :title=namedEntityTitles[category]>
            <fa fixed-width :icon="namedEntityIcon(category)" />
            <span v-html="humanEntities[category]" />
          </div>
        </div>
      </div>
      <p v-else class="text-muted text-center mb-0 col-12">
        graph is empty
      </p>
    </div>
  </v-wait>
</template>

<script>
import sum from 'lodash/sum'
import uniqueId from 'lodash/uniqueId'
import values from 'lodash/values'
import { mapState } from 'vuex'
import { AppStatus } from '../store/Neo4jModule'
import { namedEntityIcon } from '../utils/named-entities'

const DEFAULT_COUNTS = Object.freeze(
  {
    documents: 0,
    entities: {
      emails: 0,
      locations: 0,
      organizations: 0,
      people: 0
    }
  }
)

export default {
  name: 'Neo4jEntityCount',
  computed: {
    categories() {
      return Object.keys(this.namedEntities)
    },
    documents() {
      return this.projectCounts?.documents
    },
    documentTitle() {
      return `${this.$tc('widget.barometer.document', this.documents, { total: this.documents })}`
    }
    ,
    humanEntities() {
      return Object.entries(this.namedEntities).reduce((human, [key, value]) => {
        human[key] = this.humanNumber(value)
        return human
      }, {})
    },
    loader() {
      return uniqueId('loading-neo4j-entities-count-')
    },
    namedEntities() {
      const entities = this.projectCounts?.namedEntities
      if (!this.projectCounts?.namedEntities) {
        return DEFAULT_COUNTS
      }
      return {
        emails: entities.EMAIL ?? 0,
        locations: entities.LOCATION ?? 0,
        organizations: entities.ORGANIZATION ?? 0,
        people: entities.PERSON ?? 0,
      }
    },
    namedEntityTitles() {
      return Object.entries(this.namedEntities).reduce((titles, [category, count]) => {
        titles[category] = `${count} ${category}`
        return titles
      }, {})
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
    projectCounts() {
      return this.neo4jGraphCounts[this.project] || DEFAULT_COUNTS
    },
    totalEntities() {
      return sum(values(this.namedEntities))
    },
    ...mapState('neo4j', ['neo4jAppStatus', 'neo4jInitializedProjects', 'neo4jGraphCounts'])
  },
  watch: {
    async projectReady() {
      await this.refreshCounts()
    },
    async project() {
      await this.refreshCounts()
    }
  },
  async created() {
    await this.refreshCounts()
  },
  methods: {
    namedEntityIcon,
    humanNumber(n, { K = '%K', M = '%M', B = '%B' } = {}) {
      switch (true) {
        case n < 1e3:
          return n
        case n < 1e6:
          return K.replace('%', +(n / 1e3).toFixed(1))
        case n < 1e9:
          return M.replace('%', +(n / 1e6).toFixed(1))
        default:
          return B.replace('%', +(n / 1e9).toFixed(1))
      }
    },
    async refreshCounts() {
      this.$wait.start(this.loader)
      if (this.projectReady) {
        await this.$store.dispatch('neo4j/refreshGraphCounts', this.project)
      }
      this.$wait.end(this.loader)
    }
  }
}
</script>

<style lang="scss" scoped>
.widget {
  min-height: 100%;
  position: relative;

  &__spinner {
    text-align: center;
    width: 100%;
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
