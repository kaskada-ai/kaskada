function openTab(evt, tabName) {
    // Declare all variables
    var i, rootElement, tabcontent, tablinks, tabshow;

    // Get all elements with class="tabcontent" and hide them
    rootElement = evt.currentTarget.parentElement.parentElement;
    tabcontent = rootElement.getElementsByClassName("kda_tabcontent");
    for (i = 0; i < tabcontent.length; i++) {
        tabcontent[i].style.display = "none";
    }

    // Get all elements with class="tablinks" and remove the class "active"
    tablinks = rootElement.getElementsByClassName("kda_tablinks");
    for (i = 0; i < tablinks.length; i++) {
        tablinks[i].className = tablinks[i].className.replace(" kda_active", "");
    }

    // Show the current tab, and add an "active" class to the button that opened the tab
    tabshow = rootElement.getElementsByClassName(tabName);
    for (i = 0; i < tabshow.length; i++) {
        tabshow[i].style.display = "block";
    }
    evt.currentTarget.className += " kda_active";
}
