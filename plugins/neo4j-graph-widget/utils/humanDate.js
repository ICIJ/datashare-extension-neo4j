import moment from 'moment'

export function humanDate(date, locale) {
  return moment(date).locale(locale).format('YY/MM/DD')
}

export function humanTime(date, locale) {
  return moment(date).locale(locale).format('HH:mm')
}

export function humanLongDate(date, locale) {
  return moment(date).locale(locale).format('LLL')
}

export function humanShortDate(date, locale) {
  return moment(date).locale(locale).format('LL')
}

export function isDateValid(date) {
  return moment(date).isValid()
}
