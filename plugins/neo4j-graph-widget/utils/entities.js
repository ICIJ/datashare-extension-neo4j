import { PhEnvelope, PhMapPin, PhBuildings, PhUserCircle } from '@phosphor-icons/vue'

export const VARIANTS = Object.freeze({
  PERSON: 'category-person',
  ORGANIZATION: 'category-organization',
  LOCATION: 'category-location',
  EMAIL: 'category-email'
})

export const ICONS = Object.freeze({
  PERSON: PhUserCircle,
  ORGANIZATION: PhBuildings,
  LOCATION: PhMapPin,
  EMAIL: PhEnvelope
})

export function getCategoryIcon(category) {
  const icons = {
    person: ICONS.PERSON,
    persons: ICONS.PERSON,
    people: ICONS.PERSON,
    organization: ICONS.ORGANIZATION,
    organizations: ICONS.ORGANIZATION,
    location: ICONS.LOCATION,
    locations: ICONS.LOCATION,
    email: ICONS.EMAIL,
    emails: ICONS.EMAIL
  }

  return icons[category.toLowerCase()]
}

export function getCategoryVariant(category) {
  const variants = {
    person: VARIANTS.PERSON,
    persons: VARIANTS.PERSON,
    people: VARIANTS.PERSON,
    organization: VARIANTS.ORGANIZATION,
    organizations: VARIANTS.ORGANIZATION,
    location: VARIANTS.LOCATION,
    locations: VARIANTS.LOCATION,
    email: VARIANTS.EMAIL,
    emails: VARIANTS.EMAIL
  }

  return variants[category.toLowerCase()]
}

export function getCategoryColor(category) {
  return `var(--bs-category-${category})`
}
