<script setup>
import { computed, watch, onBeforeMount } from 'vue'
import random from 'lodash/random'

import WidgetNeo4jFormSyncEmpty from './WidgetNeo4jFormSyncEmpty.vue'
import WidgetNeo4jFormSyncList from './WidgetNeo4jFormSyncList.vue'

import { useCoreComponent } from '@/composables/useCoreComponent'
import { useNeo4jCore } from '@/composables/useNeo4jCore'
import { usePolling } from '@/composables/usePolling'
import { useRoute } from '@/composables/useRoute'
import useNeo4jStore from '@/stores/neo4j'

const neo4jCore = useNeo4jCore()
const neo4jStore = useNeo4jStore()
const route = useRoute()
const { registerPollOnce } = usePolling()

const FormStep = await useCoreComponent('Form/FormStep/FormStep')

const status = computed(() => neo4jStore.status)
const neo4jAppIsRunning = computed(() => status.value.isRunning)
const syncTasks = computed(() => neo4jStore.syncTasks)
const hasTasks = computed(() => !!syncTasks.value?.length)
const project = computed(() => route.params.name)
const projectReady = computed(() => neo4jAppIsRunning.value && neo4jStore.initializedProjects[project.value])

watch(projectReady, neo4jStore.refreshSyncTasks)

async function syncGraph() {
  const config = { method: 'POST', headers: { 'Content-Type': 'application/json' } }
  await neo4jCore.request(`/api/neo4j/full-imports?project=${project.value}`, config)
  registerRefreshPoll()
}

async function refreshSyncTasks() {
  await neo4jStore.refreshSyncTasks()
  await neo4jStore.refreshGraphCounts(project.value)
  return !!neo4jStore.pendingImportTasks.length
}

function registerRefreshPoll() {
  const timeout = () => random(5000, 10000)
  return registerPollOnce({ timeout, fn: refreshSyncTasks, immediate: true })
}

onBeforeMount(registerRefreshPoll)
</script>

<template>
  <form-step class="neo4-graph-tasks" title="Sync">
    <widget-neo4j-form-sync-list v-if="hasTasks" :tasks="syncTasks" @sync="syncGraph" />
    <widget-neo4j-form-sync-empty v-else @sync="syncGraph" />
  </form-step>
</template>
