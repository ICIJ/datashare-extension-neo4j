<template>
  <div>
    <!-- TODO: display error in href when in error ? -->
    <b-badge :variant="batchVariant"> {{ batchLabel }}</b-badge>
  </div>
</template>

<script>
import { AppStatus } from '../store/Neo4jModule'

export default {
  name: 'Neo4jStatusBadge',
  props: {
    status: {
      type: AppStatus
    }
  },
  computed: {
    batchVariant() { return this.badge.variant },
    batchLabel() { return this.badge.label },
    badge() {
      console.log(this.status)
      switch (this.status) {
        case AppStatus.Error:
          return { variant: "danger", label: "Error" }
        case AppStatus.Running:
          return { variant: "success", label: "Running" }
        case AppStatus.Starting:
          return { variant: "primary", label: "Starting" }
        default:
          return { variant: "secondary", label: "Stopped" }
      }
    },
  }
}
</script>
<style lang="scss" scoped></style>
