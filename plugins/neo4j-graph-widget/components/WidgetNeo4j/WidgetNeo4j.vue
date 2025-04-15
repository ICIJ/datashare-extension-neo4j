<script setup>
import { ref, computed, watch, onBeforeMount, toRef } from 'vue'
import { random } from 'lodash'

import WidgetNeo4jHeading from './WidgetNeo4jHeading.vue'
import WidgetNeo4jStatistics from './WidgetNeo4jStatistics/WidgetNeo4jStatistics.vue'
import WidgetNeo4jForm from './WidgetNeo4jForm/WidgetNeo4jForm.vue'

import { useNeo4jCore } from '@/composables/useNeo4jCore'
import { usePolling } from '@/composables/usePolling'
import { useRoute } from '@/composables/useRoute'
import useNeo4jStore, { AppStatus } from '@/stores/neo4j'

const neo4jCore = useNeo4jCore()
const route = useRoute()
const neo4jStore = useNeo4jStore()
const { registerPollOnce } = usePolling()

defineProps({
  widget: {
    type: Object,
    default: () => ({})
  }
})

const initializedProject = ref(false)
const project = computed(() => route.params.name)
const shouldStartNeo4jApp = computed(() => [AppStatus.Error, AppStatus.Stopped].includes(neo4jStore.status))

async function initProject() {
  await neo4jCore.request(`/api/neo4j/init?project=${project.value}`, { method: 'POST' })
  neo4jStore.setProjectInit({ project: project.value, initialized: true })
  initializedProject.value = true
}

async function startNeo4jAppIfNeed() {
  if (shouldStartNeo4jApp.value) {
    await startNeo4jApp()
  }
}

async function startNeo4jApp() {
  neo4jStore.setStatus(AppStatus.Starting)
  await neo4jCore.request('/api/neo4j/start', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    }
  })
  neo4jStore.setStatus(AppStatus.Running)
}

async function resfreshNeo4jAppStatus() {
  await neo4jStore.refreshStatus()
  return !neo4jStore.isRunning
}

watch(toRef(neo4jStore, 'status'), async (newVal) => {
  if (newVal && !initializedProject.value) {
    await initProject()
  }
})

onBeforeMount(async () => {
  await startNeo4jAppIfNeed()
  const fn = resfreshNeo4jAppStatus
  const timeout = () => random(5000, 10000)
  registerPollOnce({ fn, timeout, immediate: true })
})
</script>

<template>
  <div class="card card-body border-0 px-4 d-flex flex-column gap-5">
    <widget-neo4j-heading :status="neo4jStore.status" />
    <widget-neo4j-statistics />
    <widget-neo4j-form />
  </div>
</template>
