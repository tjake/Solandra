// $Id$

/**
 * Interacts with Solr's SpellCheckComponent.
 *
 * <p>Requires <tt>String#strtr</tt> defined in <tt>ajaxsolr.support.js</tt>.
 *
 * @see http://wiki.apache.org/solr/SpellCheckComponent
 *
 * @class AbstractSpellcheckWidget
 * @augments AjaxSolr.AbstractWidget
 */
AjaxSolr.AbstractSpellcheckWidget = AjaxSolr.AbstractWidget.extend(
  /** @lends AjaxSolr.AbstractSpellcheckWidget.prototype */
  {
  /**
   * The suggestions.
   *
   * @field
   * @private
   * @type Object
   * @default {}
   */
  suggestions: {},

  /**
   * Uses the top suggestion for each word to return a suggested query.
   *
   * @returns {String} A suggested query.
   */
  suggestion: function () {
    var replacePairs = {};
    for (var word in this.suggestions) {
      replacePairs[word] = this.suggestions[word][0];
    }
    return this.manager.response.responseHeader.params['spellcheck.q'].strtr(replacePairs);
  },

  beforeRequest: function () {
    if (this.manager.store.get('spellcheck').val() && this.manager.store.get('q').val()) {
      this.manager.store.get('spellcheck.q').val(this.manager.store.get('q').val());
    }
    else {
      this.manager.store.remove('spellcheck.q');
    }
  },

  afterRequest: function () {
    this.suggestions = {};

    if (this.manager.response.spellcheck && this.manager.response.spellcheck.suggestions) {
      var suggestions = this.manager.response.spellcheck.suggestions;

      for (var word in suggestions) {
        if (word == 'collation' || word == 'correctlySpelled') continue;

        this.suggestions[word] = [];
        for (var i = 0, l = suggestions[word].suggestion.length; i < l; i++) {
          if (this.manager.response.responseHeader.params['spellcheck.extendedResults']) {
            this.suggestions[word].push(suggestions[word].suggestion[i].word);
          }
          else {
            this.suggestions[word].push(suggestions[word].suggestion[i]);
          }
        }
      }

      if (AjaxSolr.size(this.suggestions)) {
        this.handleSuggestions(this.manager.response);
      }
    }
  },

  /**
   * An abstract hook for child implementations.
   *
   * <p>Allow the child to handle the suggestions without parsing the response.</p>
   */
  handleSuggestions: function () {}
});
