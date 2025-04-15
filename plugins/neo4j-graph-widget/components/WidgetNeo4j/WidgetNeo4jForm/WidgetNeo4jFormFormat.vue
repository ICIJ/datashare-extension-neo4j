<script setup>
import { useCoreComponent } from '@/composables/useCoreComponent'

const FormStep = await useCoreComponent('Form/FormStep/FormStep')
const FormFieldset = await useCoreComponent('Form/FormFieldset/FormFieldset')

const CYPHER_SHELL = 'cypher-shell'
const GRAPHML = 'graphml'

const modelValue = defineModel({ type: String, default: '' })

const availableFormats = [
  { value: CYPHER_SHELL, text: 'Cypher shell' },
  { value: GRAPHML, text: 'GraphML' }
]
</script>

<template>
  <form-step title="Format">
    <form-fieldset label="Select format" class="mb-0">
      <b-form-radio-group v-model="modelValue" :options="availableFormats" required class="py-2" />
    </form-fieldset>
  </form-step>
</template>
