<script setup>
import { useCoreComponent } from '@/composables/useCoreComponent'

const SearchBreadcrumbUri = await useCoreComponent('Search/SearchBreadcrumbUri/SearchBreadcrumbUri')

defineProps({
  uri: {
    type: String,
    required: true
  }
})
</script>

<template>
  <search-breadcrumb-uri :key="uri" :uri="uri">
    <template #label>Your graph:</template>
  </search-breadcrumb-uri>
</template>
