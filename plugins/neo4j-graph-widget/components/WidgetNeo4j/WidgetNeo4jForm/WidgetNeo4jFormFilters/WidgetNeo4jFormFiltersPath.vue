<script setup>
import { toRef, watch } from 'vue'

import { useCore } from '@/composables/useCore'
import { useCoreComponent } from '@/composables/useCoreComponent'

const modelValue = defineModel({ type: Array, default: [] })
const core = useCore()

const searchStore = core.stores.useSearchStore.inject()
const filterPath = searchStore.getFilter({ name: 'path' })

const FilterTypePath = await useCoreComponent('Filter/FilterType/FilterTypePath')
const values = toRef(filterPath, 'values')

watch(values, (v) => (modelValue.value = v), { deep: true, immediate: true })
</script>

<template>
  <filter-type-path
    :filter="filterPath"
    :collapse="false"
    actions-position-title
    hide-contextualize
    hide-count
    hide-exclude
    class="p-3"
    content-class="pb-0"
  />
</template>
