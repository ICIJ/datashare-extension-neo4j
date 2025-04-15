<script setup>
import { useCoreComponent } from '@/composables/useCoreComponent'

defineProps({
  task: {
    type: Object,
    default: () => ({})
  }
})

const DisplayStatus = await useCoreComponent('Display/DisplayStatus')
const DisplayProgress = await useCoreComponent('Display/DisplayProgress')
const DisplayDatetimeFromNow = await useCoreComponent('Display/DisplayDatetimeFromNow')
</script>

<template>
  <tr class="widget-neo4j-form-sync-list-entry">
    <td class="pe-3">
      <display-status :value="task.status" />
    </td>
    <td class="w-100 pe-3">
      <display-progress
        :value="task.progress"
        style="--display-progress-bar-width: 100%"
        class="widget-neo4j-form-sync-list-entry__progress"
      />
    </td>
    <td>
      <display-datetime-from-now :value="task.createdAt" class="ms-auto text-body-secondary text-nowrap" />
    </td>
  </tr>
</template>

<style lang="scss" scoped>
.widget-neo4j-form-sync-list-entry {
  &__progress {
    min-width: 100px;
    width: 100%;
  }

  &:deep(td) {
    vertical-align: middle;
  }
}
</style>
