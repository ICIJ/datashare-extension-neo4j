<template>
  <div class="widget--hook">
    <widget-neo4j-dump :widget="widget" />
  </div>
</template>


<script>

import { default as Widget } from '../core/WidgetNeo4jDump';
import WidgetNeo4jDump from '../components/WidgetNeo4jDump';

export default {
  name: 'WidgetHook',
  components: {
    WidgetNeo4jDump
  },
  data() {
    return {
      msg: "yooo",
      widget: new Widget()
    }
  }
}

</script>

<style lang="scss" scoped>
.widget--hook {
  width: 100%;
}
</style>
