<script setup>
import { computed } from 'vue'

import { AppStatus } from '@/stores/neo4j'

const props = defineProps({
  status: {
    type: String,
    validator: (value) => Object.values(AppStatus).includes(value)
  }
})

const badge = computed(() => {
  switch (props.status) {
    case AppStatus.Error:
      return { variant: 'danger', label: 'Error' }
    case AppStatus.Running:
      return { variant: 'success', label: 'Running' }
    case AppStatus.Starting:
      return { variant: 'primary', label: 'Starting' }
    default:
      return { variant: 'secondary', label: 'Stopped' }
  }
})
</script>

<template>
  <b-badge class="neo4j-status-badge" :variant="badge.variant">
    {{ badge.label }}
  </b-badge>
</template>
