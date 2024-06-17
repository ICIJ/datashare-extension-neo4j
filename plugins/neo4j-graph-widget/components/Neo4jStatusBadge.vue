<template>
  <b-badge :variant="batchVariant">{{ batchLabel }}</b-badge>
</template>

<script>
import { AppStatus } from '../store/Neo4jModule'

export default {
  name: 'Neo4jStatusBadge',
  props: {
    status: {
      type: 'string',
      validator: (value) => Object.values(AppStatus).includes(value)
    }
  },
  computed: {
    batchVariant() { return this.badge.variant },
    batchLabel() { return this.badge.label },
    badge() {
      switch (this.status) {
        case AppStatus.Error:
          return { variant: "danger", label: "ERROR" }
        case AppStatus.Running:
          return { variant: "success", label: "RUNNING" }
        case AppStatus.Starting:
          return { variant: "primary", label: "STARTING" }
        default:
          return { variant: "secondary", label: "STOPPED" }
      }
    },
  }
}
</script>
<style lang="scss" scoped></style>
