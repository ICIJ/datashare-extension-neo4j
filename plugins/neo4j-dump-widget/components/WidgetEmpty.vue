<template>
  <div class="widget widget--empty"></div>
</template>

<script>
/**
 * A placeholder widget for the insights page. This widget is not intended to be used directly.
 */
export default {
  name: 'WidgetEmpty',
  props: {
    /**
     * The widget definition object.
     */
    widget: {
      type: Object
    }
  }
}
</script>

<style lang="scss" scoped>
// TODO: fix this
// @use 'sass:math';

.widget--empty {
  width: 100%;

  &:before {
    content: '';
    display: block;
    // TODO: fix this
    // padding-top: math.div(9, 16) * 100%;
    padding-top: 0.5 * 100%;
  }

  &:after {
    // TODO: fix this
    // color: $text-muted;
    color: grey;
    content: 'This is an empty widget';
    font-weight: lighter;
    left: 0;
    position: absolute;
    right: 0;
    text-align: center;
    top: 50%;
    transform: translateY(-50%);
  }
}
</style>
