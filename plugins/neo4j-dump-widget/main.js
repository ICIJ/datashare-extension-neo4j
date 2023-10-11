import dumpWidgetFactory from './core/WidgetNeo4jDump'
import { default as neo4jStoreBuilder } from './store/Neo4jModule'
import { CoreExtension } from './core/CoreExtension'
     
document.addEventListener('datashare:ready', async ({ detail }) => {
  if (!detail.core.store.hasModule('neo4j')) {
    const extension = new CoreExtension(detail.core)
    // Register the extension under this.$neo4jCore
    extension.useExtension()
    const module = neo4jStoreBuilder(extension)
    detail.core.store.registerModule('neo4j', module)
  }
  detail.core.registerWidget({
    name: 'WidgetNeo4jDump',
    type(WidgetEmptyCls) {
      return dumpWidgetFactory(WidgetEmptyCls)
    },
    order: 36,
  })
}, false)
