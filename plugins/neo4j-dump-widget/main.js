import WidgetHook from './components/WidgetHook.vue'
import dumpWidgetFactory from './core/WidgetNeo4jDump'

document.addEventListener('datashare:ready', async ({ detail }) => {
  detail.core.registerHook({
    target: 'landing.form.project:before',
    definition: WidgetHook
  })
  detail.core.registerWidget({
    name: 'WidgetNeo4jDump',
    type(WidgetEmptyCls) {
      return dumpWidgetFactory(WidgetEmptyCls)
    }
  })
}, false)
