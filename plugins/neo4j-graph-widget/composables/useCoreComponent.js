import { useCore } from '@/composables/useCore'

export function useCoreComponent(path) {
  const core = useCore()
  return core.findComponent(path)
}
