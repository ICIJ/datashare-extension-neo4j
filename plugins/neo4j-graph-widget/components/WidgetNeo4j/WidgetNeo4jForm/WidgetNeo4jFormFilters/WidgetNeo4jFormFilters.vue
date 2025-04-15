<script setup>
import WidgetNeo4jFormFiltersContentType from './WidgetNeo4jFormFiltersContentType.vue'
import WidgetNeo4jFormFiltersPath from './WidgetNeo4jFormFiltersPath.vue'

import { useCore } from '@/composables/useCore'
import { useCoreComponent } from '@/composables/useCoreComponent'

const contentTypes = defineModel('contentTypes', { type: Array, default: [] })
const paths = defineModel('paths', { type: Array, default: [] })
const { project } = defineProps({ project: { type: String, required: true } })

const core = useCore()
const searchStore = core.stores.useSearchStore.inject()
const FormStep = await useCoreComponent('Form/FormStep/FormStep')

searchStore.setIndices([project])
</script>

<template>
  <form-step title="Filters" content-class="bg-transparent p-0">
    <div class="d-flex flex-column gap-3">
      <widget-neo4j-form-filters-path v-model="paths" />
      <widget-neo4j-form-filters-content-type v-model="contentTypes" />
    </div>
  </form-step>
</template>
