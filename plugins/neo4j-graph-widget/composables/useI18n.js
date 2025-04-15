import { useCore } from './useCore'

export function useI18n() {
  const core = useCore()
  return core.i18n.global
}
