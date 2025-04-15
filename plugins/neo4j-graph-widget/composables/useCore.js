import { getCurrentInstance } from 'vue'

export function useCore() {
  const { appContext } = getCurrentInstance()
  const { $core } = appContext.config.globalProperties
  return $core
}
