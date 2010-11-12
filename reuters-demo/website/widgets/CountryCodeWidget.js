(function ($) {

AjaxSolr.CountryCodeWidget = AjaxSolr.AbstractFacetWidget.extend({
  afterRequest: function () {
    $(this.target).empty();

    var maps = {
      world: 'view the World',
      africa: 'view Africa',
      asia: 'view Asia',
      europe: 'view Europe',
      middle_east: 'view the Middle East',
      south_america: 'view South America',
      usa: 'view North America'
    };
    $(this.target).append(AjaxSolr.theme('select_tag', 'region', AjaxSolr.theme('options_for_select', maps)));

    $(this.target).find('#region').change(function () {
      $(this.target).find('img').hide();
      $('#' + self.id + $(this).val()).show();
    });

    var maxCount = 0;
    var options = { '': '--select--' };
    for (var facet in this.manager.response.facet_counts.facet_fields[this.field]) {
      if (facet.length == 2) { // only display country codes
        var count = this.manager.response.facet_counts.facet_fields[this.field][facet];
        if (count > maxCount) {
          maxCount = count;
        }
        options[facet] = facet + ' (' + count + ')';
      }
    }
    $(this.target).append(AjaxSolr.theme('select_tag', 'country', AjaxSolr.theme('options_for_select', options)));

    var self = this;
    $(this.target).find('#country').change(function () {
      var value = $(this).val();
      if (value && self.add(value)) {
        self.manager.doRequest(0);
      }
    });

    var chd = [];
    var chld = '';
    for (var facet in this.manager.response.facet_counts.facet_fields[this.field]) {
      if (facet.length == 2) { // only display country codes
        chd.push(parseInt(this.manager.response.facet_counts.facet_fields[this.field][facet] / maxCount * 100) + '.0');
        chld += facet;
      }
    }
    for (var value in maps) {
      var src = 'http://chart.apis.google.com/chart?chco=f5f5f5,edf0d4,6c9642,365e24,13390a&chd=t:' + chd.join(',') + '&chf=bg,s,eaf7fe&chtm=' + value + '&chld=' + chld + '&chs=350x180&cht=t';
      $('<img/>').attr('id', this.id + value).showIf(value == 'world').attr('src', src).appendTo(this.target);
    }
  }
});

})(jQuery);
