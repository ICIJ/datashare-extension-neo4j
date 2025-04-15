<script setup>
const emit = defineEmits(['sync'])
</script>

<template>
  <div>
    <p>The graph is empty.</p>
    <b-button variant="action" @click="emit('sync')">Sync</b-button>
  </div>
</template>
