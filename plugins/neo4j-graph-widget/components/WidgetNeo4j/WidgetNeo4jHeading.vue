<script setup>
import WidgetNeo4jStatus from './WidgetNeo4jStatus.vue'

import IPhPolygon from '~icons/ph/polygon'

defineProps({
  status: {
    type: Object,
    default: () => ({})
  }
})
</script>

<template>
  <div class="d-flex gap-3 align-items-center mb-3">
    <i-ph-polygon size="2rem" />
    <h4 class="p-0 m-0">Explore graph with Neo4j</h4>
    <widget-neo4j-status :status="status" />
  </div>
</template>
