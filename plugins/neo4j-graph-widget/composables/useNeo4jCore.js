import { useCore } from './useCore'

import { CoreExtension } from '@/core/CoreExtension'

export function useNeo4jCore() {
  const core = useCore()
  return new CoreExtension(core)
}
