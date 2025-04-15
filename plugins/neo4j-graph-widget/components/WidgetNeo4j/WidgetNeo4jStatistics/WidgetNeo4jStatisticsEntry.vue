<script setup>
import { computed } from 'vue'

import { useCore } from '@/composables/useCore'
import { getCategoryIcon, getCategoryVariant } from '@/utils/entities'

const core = useCore()

const WidgetBarometer = await core.findComponent('Widget/WidgetBarometer')

const { category } = defineProps({
  category: {
    type: String,
    required: true
  },
  value: {
    type: Number,
    default: 0
  }
})

const icon = computed(() => getCategoryIcon(category))
const variant = computed(() => getCategoryVariant(category))
const label = computed(() => category)
</script>

<template>
  <widget-barometer :icon="icon" :variant="variant" :border-variant="variant" :label="label" :value="value" />
</template>
