(function ($) {

AjaxSolr.AutocompleteWidget = AjaxSolr.AbstractFacetWidget.extend({
  init: function () {
    var self = this;

    $(this.target).find('input').bind('keydown', function(e) {
      if (self.requestSent === false && e.which == 13) {
        var value = $(this).val();
        if (value && self.add(value)) {
          self.manager.doRequest(0);
        }
      }
    });
  },

  afterRequest: function () {
    $(this.target).find('input').val('');

    var self = this;

    var callback = function (response) {
      var list = [];
      for (var i = 0; i < self.fields.length; i++) {
        var field = self.fields[i];
        for (var facet in response.facet_counts.facet_fields[field]) {
          list.push({
            field: field,
            value: facet,
            text: facet + ' (' + response.facet_counts.facet_fields[field][facet] + ') - ' + field
          });
        }
      }

      self.requestSent = false;
      $(self.target).find('input').autocomplete(list, {
        formatItem: function(facet) {
          return facet.text;
        }
      }).result(function(e, facet) {
        self.requestSent = true;
        if (self.manager.store.addByValue('fq', facet.field + ':' + facet.value)) {
          self.manager.doRequest(0);
        }
      });
    } // end callback

    var params = [ 'q=*:*&facet=true&facet.limit=-1&facet.mincount=1&json.nl=map' ];
    for (var i = 0; i < this.fields.length; i++) {
      params.push('facet.field=' + this.fields[i]);
    }
    jQuery.getJSON(this.manager.solrUrl + 'select?' + params.join('&') + '&wt=json&json.wrf=?', {}, callback);
  }
});

})(jQuery);
