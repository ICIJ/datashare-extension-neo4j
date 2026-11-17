import { useCore } from '@/composables/useCore'

export function useApi() {
  const { api } = useCore()
  return api
}
