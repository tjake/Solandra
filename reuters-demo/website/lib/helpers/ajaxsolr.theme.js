// $Id$

/**
 * Requires ajaxsolr.support.js.
 */

/**
 * Accepts a container and returns a string of option tags.
 *
 * <p>If the container is an object, the object's properties serve as option
 * values and the values of the object's properties serve as option text.</p>
 *
 * <p>If the container is an array: Given a container where the elements are
 * two-element arrays, the first elements serve as option text and the second
 * elements serve as option values.</p>
 *
 * <p>If <tt>selected</tt> is specified, the matching option value or element
 * will get the selected option-tag. <tt>selected</tt> may also be an array of
 * values to be selected when using a multiple select.</p>
 *
 * <p>From Ruby on Rails.</p>
 *
 * @param {Array|Object} container
 * @param {Array|String} selected
 * @returns {String} The option tags.
 */
AjaxSolr.theme.prototype.options_for_select = function (container, selected) {
  var tags = [];

  var options = [];
  if (AjaxSolr.isArray(container)) {
    options = container;
  }
  else {
    for (var value in container) {
      options.push([ container[value], value ]);
    }
  }

  for (var i = 0, l = options.length; i < l; i++) {
    var text, value;

    if (AjaxSolr.isArray(options[i])) {
      text = options[i][0].toString(), value = options[i][1].toString();
    }
    else {
      text = options[i].toString(), value = options[i].toString();
    }

    var selectedAttribute = AjaxSolr.optionValueSelected(value, selected) ? ' selected="selected"' : '';
    tags.push('<option value="' + value.htmlEscape() +'"' + selectedAttribute + '>' + text.htmlEscape() + '</option>');
  }

  return tags.join('\n');
};

/**
 * <p>From Ruby on Rails.</p>
 */
AjaxSolr.theme.prototype.select_tag = function (name, optionTags, options) {
  options = options || {};
  var htmlName = options.multiple && !name.endsWith('[]') ? name + '[]' : name;
  options.name = options.name || htmlName;
  options.id = options.id || name.sanitizeToId();
  return AjaxSolr.theme('content_tag_string', 'select', optionTags, options);
};

/**
 * <p>From Ruby on Rails.</p>
 */
AjaxSolr.theme.prototype.content_tag_string = function (name, content, options, escape) {
  var tagOptions = '';

  if (escape === undefined) {
    escape = true;
  }

  if (options) {
    tagOptions = AjaxSolr.tagOptions(options, escape)
  }

  return '<' + name + tagOptions + '>' + content + '</' + name + '>';
};

/**
 * <p>From Ruby on Rails.</p>
 *
 * @field
 * @private
 */
AjaxSolr.booleanAttributes = [ 'disabled', 'readonly', 'multiple', 'checked' ];

/**
 * <p>From Ruby on Rails.</p>
 *
 * @static
 */
AjaxSolr.optionValueSelected = function (value, selected) {
  if (AjaxSolr.isArray(selected)) {
    return AjaxSolr.inArray(value, selected) != -1;
  }
  else {
    return selected == value;
  }
};

/**
 * <p>From Ruby on Rails.</p>
 *
 * @static
 */
AjaxSolr.tagOptions = function (options, escape) {
  options = options || {};

  if (escape === undefined) {
    escape = true;
  }

  var attrs = [];

  if (escape) {
    for (var key in options) {
      if (AjaxSolr.inArray(key, AjaxSolr.booleanAttributes) != -1) {
        if (options[key]) {
          attrs.push(key + '="' + key + '"');
        }
      }
      else {
        if (options[key]) {
          attrs.push(key + '="' + options[key].escapeOnce() + '"');
        }
      }
    }
  }
  else {
    for (var key in options) {
      attrs.push(key + '="' + options[key] + '"');
    }
  }

  if (attrs.length) {
    return ' ' + attrs.sort().join(' ');
  }

  return '';
};
