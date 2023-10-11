import { faCheck } from '@fortawesome/free-solid-svg-icons/faCheck'
import { faExclamation } from '@fortawesome/free-solid-svg-icons/faExclamation'
import { faTimes } from '@fortawesome/free-solid-svg-icons/faTimes'

import settings from '../utils/settings'

/**
 * Slugify a string value.
 *
 * @param {string} [value=''] - The string to be slugified.
 * @return {string} - The slugified string.
 */
function slugger(value = '') {
  return value
    .toLowerCase()
    .trim()
    .replace(/[\u2000-\u206F\u2E00-\u2E7F\\'!"#$%&()*+,./:;<=>?@[\]^`{|}~]/g, '')
    .replace(/\s/g, '-')
}


function toVariant(string = '', defaultVariant = 'darker', prefix = '') {
  return prefix + settings.variantsMap[slugger(string).toLowerCase()] || defaultVariant
}

function toVariantIcon(string = '', defaultVariant = 'darker') {
  const variant = toVariant(string, defaultVariant)
  const icons = {
    success: faCheck,
    danger: faTimes,
    warning: faExclamation
  }
  return icons[variant]
}

function toVariantColor(string = '', defaultVariant = 'darker') {
  const variant = toVariant(string, defaultVariant)
  const style = getComputedStyle(document.body)
  return style.getPropertyValue(`--${variant}`) || '#eee'
}

export {
  slugger,
  toVariant,
  toVariantIcon,
  toVariantColor,
}
