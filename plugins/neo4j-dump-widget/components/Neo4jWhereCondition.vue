<template>
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
      <b-row>
        <b-col v-if="isLocalPath">
          <b-button
            v-b-modal="`treeview-${condition.name}`">
            Choose
          </b-button>
          <b-modal
            :id="`treeview-${condition.name}`"
            @ok="pathToValue"
            lazy scrollable
            size="lg">
            <component :is="treeView"
              v-model="selectedPath"
              :id="`treeview-${condition.name}`"
              :projects="[project]"
              selectable
              count>
            </component>
          </b-modal>
        </b-col>
        <b-col v-if="!isLocalPath || condition.value !== null">
          <b-form-input
            v-model="condition.value"
            :id="`input-${condition.name}`"
            :type="conditionType"
            :disabled="isLocalPath">
            <!-- TODO: add tooltip when disabled -->
          </b-form-input>
        </b-col>
      </b-row>
    </b-col>
    <b-col lg="1" class="pb-2">
      <b-button v-on:click="$emit('removeCondition')">x</b-button>
    </b-col>
  </b-row>
</template>
  
  
<script>

import Neo4jWhereCondition from "../core/Neo4jWhereCondition"


export default {
  name: 'Neo4jWhereCondition',
  props: {
    condition: {
      type: Neo4jWhereCondition
    },
    project: {
      type: String
    }
  },
  data() {
    return {
      conditionNames: {
        string: ["isEqualTo", "startsWith", "endsWith"],
        localPath: ["isEqualTo", "startsWith"],
      },
      selectedPath: this.$config.get('mountedDataDir') || this.$config.get('dataDir'),
    }
  },
  computed: {
    availableConditionNames() {
      return this.conditionNames[this.conditionType]
    },
    availableProperties() {
      return Object.keys(this.condition.properties)
    },
    conditionType() { return this.condition.properties[this.condition.property].type },
    isLocalPath() { return this.conditionType === 'localPath' }
  },
  methods: {
    pathToValue() {
      this.condition.value = this.selectedPath
    },
    resetCondition() {
      this.condition.conditionName = null
      this.condition.value = null
      this.condition.not = null
    },
    async treeView() {
      return await this.$core.findComponent('TreeView')
    },
  }
}
</script>
  
<style lang="scss" scoped>
.text-black {
  color: black
}
</style>
