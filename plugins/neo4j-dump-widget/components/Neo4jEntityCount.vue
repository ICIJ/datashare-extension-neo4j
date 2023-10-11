<template>
  <v-wait :for="loader" transition="fade">
    <div slot="waiting" class="widget__spinner">
      <fa icon="circle-notch" spin size="2x" />
    </div>
    <div class="widget__content text-center">
      <div v-if="documents > 0" class="row">
        <fa icon="hdd" class="widget__icon" />
        <strong :title="documents">
          {{ $tc('widget.barometer.document', documents, { total: humanNumber(documents, $t('human.number')) }) }}
        </strong>
      </div>
      <p v-else class="text-muted text-center mb-0 col-12">
        No documents in neo4j
      </p>
      <div v-if="totalEntities > 0" class="row">
        <div
          v-for="category in categories"
          :key="category"
          class="widget__content__count col-3"
          :class="{ 'widget__content__count--muted': !namedEntities[category] }">
          <fa fixed-width :icon="category | namedEntityIcon" class="mr-1" />
          <span v-html="humanEntities[category]" />
        </div>
      </div>
      <p v-else class="text-muted text-center mb-0 col-12">
        {{ $t('widget.noEntities') }}
      </p>
    </div>
  </v-wait>
</template>

<script>
import { sum, uniqueId, values } from 'lodash'
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
  filters: {
    namedEntityIcon
  },
  computed: {
    categories() {
      return Object.keys(this.namedEntities)
    },
    documents() {
      return this.projectCounts?.documents
    },
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
    // ,
    // namedEntityIcon(category) {
    //   const icons = {
    //     emails: faEnvelope,
    //     email: faEnvelope,
    //     locations: faMapMarkerAlt,
    //     location: faMapMarkerAlt,
    //     organizations: faBuilding,
    //     organization: faBuilding,
    //     people: faIdCardAlt,
    //     person: faIdCardAlt
    //   }
    //   return icons[category.toLowerCase()]
    // }
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
