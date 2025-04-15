<script setup>
import { ref, computed } from 'vue'

import WidgetNeo4jFormBreadcrumb from './WidgetNeo4jFormBreadcrumb.vue'
import WidgetNeo4jFormFormat from './WidgetNeo4jFormFormat.vue'
import WidgetNeo4jFormConfirmModal from './WidgetNeo4jFormConfirmModal.vue'
import WidgetNeo4jFormSync from './WidgetNeo4jFormSync/WidgetNeo4jFormSync.vue'
import WidgetNeo4jFormFilters from './WidgetNeo4jFormFilters/WidgetNeo4jFormFilters.vue'

import { useNeo4jCore } from '@/composables/useNeo4jCore'
import { useCore } from '@/composables/useCore'
import { useRoute } from '@/composables/useRoute'
import { useNeo4jStore } from '@/stores/neo4j'

const core = useCore()
const neo4jCore = useNeo4jCore()
const neo4jStore = useNeo4jStore()
const route = useRoute()
const searchStore = core.stores.useSearchStore.disposable()

const FormCreation = await core.findComponent('Form/FormCreation')

const CYPHER_SHELL = 'cypher-shell'
const MATCH_DOC_NODE = {
  path: {
    nodes: [
      {
        name: 'doc',
        labels: ['Document']
      }
    ]
  }
}

const project = computed(() => route.params.name)
const uri = computed(() => searchStore.stringifyBaseRouteQuery)

const dumpFormat = ref(CYPHER_SHELL)
const contentTypes = ref([])
const paths = ref([])
const showConfirmModal = ref(false)

const isServer = computed(() => core.mode?.modeName?.toUpperCase() === 'SERVER')
const importIndex = computed(() => 0 + !isServer.value)
const formatIndex = computed(() => 1 + !isServer.value)
const filtersIndex = computed(() => 2 + !isServer.value)

function toNestedWhereClause(values, field = 'or') {
  if (values.length === 0) return null
  if (values.length === 1) return values[0]
  return { [field]: values }
}

const dumpContentTypesQuery = computed(() => {
  const values = contentTypes.value.map(contentTypeToWhere)
  return toNestedWhereClause(values, 'or')
})

const dumpPathsQuery = computed(() => {
  const values = paths.value.map(pathToWhere)
  return toNestedWhereClause(values, 'or')
})

const dumpQuery = computed(() => {
  const values = [dumpContentTypesQuery.value, dumpPathsQuery.value].filter(Boolean)
  const where = toNestedWhereClause(values, 'and')
  return where ? { queries: [{ matches: [MATCH_DOC_NODE], where }] } : {}
})

function reset() {
  dumpFormat.value = CYPHER_SHELL
  paths.value = [core.config.get('mountedDataDir') || core.config.get('dataDir')]
  contentTypes.value = []
  searchStore.reset()
}

async function dumpGraph() {
  const form = document.createElement('form')
  form.method = 'POST'
  form.action = neo4jCore.getFullUrl(`/api/neo4j/graphs/dump?project=${project.value}`)
  form.target = '_blank'

  const format = dumpFormat.value
  const query = dumpQuery.value
  const input = document.createElement('input')
  input.type = 'hidden'
  input.name = 'dumpRequest'
  input.value = JSON.stringify({ format, query })

  form.appendChild(input)
  document.body.appendChild(form)
  form.submit()
  document.body.removeChild(form)
}

function contentTypeToWhere(type) {
  return {
    isEqualTo: {
      property: {
        variable: 'doc',
        name: 'contentType'
      },
      value: {
        literal: type
      }
    }
  }
}

function pathToWhere(path) {
  return {
    startsWith: {
      property: {
        variable: 'doc',
        name: 'path'
      },
      value: {
        literal: path
      }
    }
  }
}

async function confirmExport() {
  showConfirmModal.value = true
}
</script>

<template>
  <form-creation submit-label="Export" :valid="neo4jStore.isRunning" @submit="confirmExport" @reset="reset">
    <div class="d-flex flex-column gap-4 mb-3">
      <widget-neo4j-form-confirm-modal v-model="showConfirmModal" @ok="dumpGraph" @hidden="showConfirmModal = false" />
      <widget-neo4j-form-sync :index="importIndex" />
      <widget-neo4j-form-format v-model="dumpFormat" :index="formatIndex" />
      <widget-neo4j-form-filters
        v-model:content-types="contentTypes"
        v-model:paths="paths"
        :index="filtersIndex"
        :project="project"
        collapse
      />
      <widget-neo4j-form-breadcrumb :uri="uri" />
    </div>
  </form-creation>
</template>
