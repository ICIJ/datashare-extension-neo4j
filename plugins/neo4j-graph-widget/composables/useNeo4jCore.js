import { useApi } from './useApi'

import { CoreExtension } from '@/core/CoreExtension'

export function useNeo4jCore() {
  const api = useApi()
  return new CoreExtension(api)
}
