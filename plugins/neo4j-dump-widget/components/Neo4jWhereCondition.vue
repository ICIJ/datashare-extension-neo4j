<template>
  <!-- TODO: write methods to simplify this mess.... -->
  <b-row>
    <b-col lg="3">
      <b-form-select v-model="condition.property" :options="availableProperties" :id="`property-${condition.name}`"
        @change="resetCondition">
      </b-form-select>
    </b-col>
    <b-col lg="1" v-if="condition.property !== null">
      <b-row>
        <b-form-checkbox v-model="condition.not" name="check-button" :id="`not-${condition.name}`" switch>
        </b-form-checkbox>
      </b-row>
      <b-row>NOT</b-row>
    </b-col>
    <b-col lg="3" v-if="condition.property !== null">
      <b-form-select v-model="condition.conditionName" :options="availableConditionNames"></b-form-select>
    </b-col>
    <b-col lg="4" v-if="condition.conditionName !== null">
      <b-form-input v-model="condition.value" :id="`type-${condition.name}`" :type="conditionType"></b-form-input>
    </b-col>
    <b-col lg="1" class="pb-2">
      <b-button v-on:click="$emit('removeCondition')">x</b-button>
    </b-col>
  </b-row>
</template>
  
  
<script>

import Neo4jWhereCondition from "../store/Neo4jWhereCondition"


export default {
  name: 'Neo4jWhereCondition',
  props: {
    condition: {
      type: Neo4jWhereCondition
    }
  },
  data() {
    // TODO: maybe we should { ...condition } to simplify access to child attributes
    return {
      conditionNames: {
        string: ["isEqualTo", "startsWith", "endsWith"]
      },
      selectedType: null,
      foo: null
    }
  },
  computed: {
    availableProperties() {
      return Object.keys(this.condition.properties)
    },
    conditionType() { return this.condition.properties[this.condition.property].type },
    availableConditionNames() {
      return this.conditionNames[this.conditionType]
    }
  },
  methods: {
    resetCondition() {
      this.condition.conditionName = null
      this.condition.value = null
      this.condition.not = null
    }
  }
}
</script>
  
<style lang="scss" scoped></style>
  