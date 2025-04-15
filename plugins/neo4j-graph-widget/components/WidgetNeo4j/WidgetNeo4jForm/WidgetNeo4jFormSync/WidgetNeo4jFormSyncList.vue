<script setup>
import { computed } from 'vue'

import WidgetNeo4jFormSyncListLast from './WidgetNeo4jFormSyncListLast.vue'
import WidgetNeo4jFormSyncListEntry from './WidgetNeo4jFormSyncListEntry.vue'

const TASK_STATUS = Object.freeze({
  Created: 'CREATED',
  Queued: 'QUEUED',
  Running: 'RUNNING',
  Retry: 'RETRY',
  Error: 'ERROR',
  Done: 'DONE',
  Cancelled: 'CANCELLED'
})

const { tasks } = defineProps({
  tasks: {
    type: Array,
    default: () => []
  }
})

const emit = defineEmits(['sync'])

const latestDone = computed(() => tasks?.find((t) => t.status === TASK_STATUS.Done) ?? null)
</script>

<template>
  <div class="widget-neo4-form-sync-list d-flex flex-column gap-3">
    <widget-neo4j-form-sync-list-last v-if="latestDone" :task="latestDone" />
    <div class="widget-neo4-form-sync-list__content p-3 d-flex flex-column gap-3 bg-tertiary-subtle rounded">
      <table>
        <tbody>
          <widget-neo4j-form-sync-list-entry v-for="task in tasks" :key="task.id" :task="task" />
        </tbody>
      </table>
    </div>
    <b-button variant="action" class="align-self-start" @click="emit('sync')">Sync</b-button>
  </div>
</template>

<style lang="scss" scoped>
.widget-neo4-form-sync-list {
  &__content {
    height: 10rem;
    overflow-y: scroll;
  }
}
</style>
