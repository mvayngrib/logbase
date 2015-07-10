
var ArrayProto = Array.prototype

/**
 * returns the arguments after the first (collapsing them if they're an array)
 * @param  {Arguments|Array} arguments
 * @example
 *    param tags might be an array or separate arguments
 *    function tag(obj, ...tags) {
 *      rest(arguments).forEach(obj.tagOne, obj)
 *    }
 * @return {Array}
 */
module.exports = function rest () {
  return ArrayProto.concat.apply([], ArrayProto.slice.call(arguments, 1))
}
