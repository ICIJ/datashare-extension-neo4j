<script setup>
import { PhPolygon } from '@phosphor-icons/vue'

import WidgetNeo4jStatus from './WidgetNeo4jStatus.vue'

defineProps({
  status: {
    type: Object,
    default: () => ({})
  }
})
</script>

<template>
  <div class="d-flex gap-3 align-items-center mb-3">
    <PhPolygon size="2rem" />
    <h4 class="p-0 m-0">Explore graph with Neo4j</h4>
    <widget-neo4j-status :status="status" />
  </div>
</template>
