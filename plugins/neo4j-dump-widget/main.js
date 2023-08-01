import WidgetHook from "./components/WidgetHook.vue"

document.addEventListener('datashare:ready', async ({ detail }) => {
  detail.core.registerHook({
    target: 'landing.form.project:before',
    definition: WidgetHook
  })
}, false)
