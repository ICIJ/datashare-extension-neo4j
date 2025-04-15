<script setup>
import { PhFiles } from '@phosphor-icons/vue'
import { computed, watch } from 'vue'

import WidgetNeo4jStatisticsEntry from './WidgetNeo4jStatisticsEntry.vue'

import { useWait } from '@/composables/useWait'
import { useRoute } from '@/composables/useRoute'
import { useCore } from '@/composables/useCore'
import useNeo4jStore from '@/stores/neo4j'

const core = useCore()
const route = useRoute()
const { waitFor, loaderId } = useWait()
const neo4jStore = useNeo4jStore()

const AppSpinner = await core.findComponent('AppSpinner/AppSpinner')
const AppWait = await core.findComponent('AppWait/AppWait')
const WidgetBarometer = await core.findComponent('Widget/WidgetBarometer')

const project = computed(() => route.params.name)
const projectCounts = computed(() => neo4jStore.graphCounts[project.value])

const entities = computed(() => {
  const {
    EMAIL: emails = 0,
    LOCATION: locations = 0,
    ORGANIZATION: organizations = 0,
    PERSON: people = 0
  } = projectCounts.value?.namedEntities ?? {}

  return { emails, locations, organizations, people }
})

const isProjectReady = computed(() => neo4jStore.isRunning && neo4jStore.isProjectInitialized(project.value))

// The refreshCounts function updates the counts from the store for the current project
const refreshCounts = waitFor(async () => {
  if (isProjectReady.value) {
    await neo4jStore.refreshGraphCounts(project.value)
  }
})

// Watch changes to isProjectReady and refresh counts accordingly
watch(isProjectReady, refreshCounts, { immediate: true })
</script>

<template>
  <app-wait :for="loaderId" class="widget-neo4j-statistics">
    <template #waiting>
      <app-spinner class="p-3 m-auto" />
    </template>
    <h5 class="h6 mb-4">Graph statistics</h5>
    <div class="widget-neo4j-statistics__grid">
      <widget-barometer label="documents" :value="projectCounts?.documents" :icon="PhFiles" border-variant="light" />
      <widget-neo4j-statistics-entry
        v-for="(value, category) in entities"
        :key="category"
        :category="category"
        :value="value"
      />
    </div>
  </app-wait>
</template>

<style lang="scss" scoped>
.widget-neo4j-statistics {
  &__grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 2rem;
  }
}
</style>
