import { getCurrentInstance } from 'vue'

export function useRoute() {
  const { appContext } = getCurrentInstance()
  const { $route } = appContext.config.globalProperties
  return $route
}
