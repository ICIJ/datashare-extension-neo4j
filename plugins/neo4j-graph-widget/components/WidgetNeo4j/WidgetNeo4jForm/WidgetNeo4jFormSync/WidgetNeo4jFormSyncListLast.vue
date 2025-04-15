<script setup>
import { useCoreComponent } from '@/composables/useCoreComponent'

defineProps({
  task: {
    type: Object,
    default: () => ({})
  }
})

const DisplayDatetimeLong = await useCoreComponent('Display/DisplayDatetimeLong')
</script>

<template>
  <div class="text-body-secondary">Last sync on <display-datetime-long :value="task.createdAt" /></div>
</template>
