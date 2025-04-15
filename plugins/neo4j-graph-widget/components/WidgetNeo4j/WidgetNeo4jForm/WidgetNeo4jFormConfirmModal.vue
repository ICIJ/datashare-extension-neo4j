<script setup>
import { useCoreComponent } from '@/composables/useCoreComponent'
import useNeo4jStore from '@/stores/neo4j'

const neo4jStore = useNeo4jStore()
const AppModal = await useCoreComponent('AppModal/AppModal')
const DisplayNumber = await useCoreComponent('Display/DisplayNumber')
const dumpDocUrl = 'https://neo4j.com/docs/operations-manual/current/backup-restore/offline-backup/'
</script>

<template>
  <app-modal title="Downloading the graph" ok-title="Download">
    <p>
      A maximum of <display-number :value="neo4jStore.dumpLimit" /> documents and their related named entities will be
      exported. To export the entire graph, proceed to a <a :href="dumpDocUrl">full DB dump</a> or ask the system
      administrator for it.
    </p>
  </app-modal>
</template>
