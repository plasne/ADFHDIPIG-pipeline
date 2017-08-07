
function getParameterByName(name, url) {
    if (!url) url = window.location.href;
    name = name.replace(/[\[\]]/g, "\\$&");
    const regex = new RegExp("[?&]" + name + "(=([^&#]*)|&|#|$)"),
        results = regex.exec(url);
    if (!results) return null;
    if (!results[2]) return '';
    return decodeURIComponent(results[2].replace(/\+/g, " "));
}

function isProcessing(state) {
    if (state) {
        $(".dataTables_processing", $("#logs table").closest(".dataTables_wrapper")).show();
    } else {
        $(".dataTables_processing", $("#logs table").closest(".dataTables_wrapper")).hide();
    }
}

function query(table, datasets) {
    isProcessing(true);
    $.ajax({
        url: "/slices?datasets=" + datasets,
        json: true
    }).done(function(logs, status, xhr) {
        table.clear();
        table.rows.add(logs);
        table.draw();
        isProcessing(false);
    }).fail(function(xhr, status, error) {
        isProcessing(false);
        alert("fail");
    });
}

function field(name, value) {
    const field = $("<div />");
    $("<div />").text(name + ":").appendTo(field);
    $("<div />").addClass("indent").text(value).appendTo(field);
    return field;
}

function detail(data) {

    // get the fetching container
    const container = $("<div />");
    const outer = $("<div />").addClass("detail").appendTo(container);
    const inner = $("<div />").addClass("detail-items").appendTo(outer);
    $("<div />").text("fetching details, please wait...").appendTo(inner);

    // request the data
    $.ajax({
        url: "/slice?dataset=" + data.dataset + "&start=" + moment(data.start).valueOf(),
        json: true,
        cache: false
    }).done(function(slice, status, xhr) {

        inner.empty();
        field("Error Message", slice.errorMessage).appendTo(inner);

    }).fail(function(error) {
        alert(error);
    });

    return container.html();
}

function build() {

    // define the table
    const table = $("#logs > table").DataTable({
        columns: [
            {
                title: "dataset",
                data: "dataset"
            },
            {
                title: "Status",
                data: "status",
                width: "60px",
                className: "centered"
            },
            {
                title: "State",
                data: "state",
                width: "60px",
                className: "centered"
            },
            {
                title: "Retry Count",
                data: "retryCount",
                width: "20px",
                className: "centered"
            },
            {
                title: "Long Retry Count",
                data: "longRetryCount",
                width: "20px",
                className: "centered"
            },
            {
                title: "Updated",
                data: "statusUpdateTimestamp",
                width: "180px",
                render: function(data, type, row) {
                    if (type === "display") {
                        return moment(data).utc().format("M-D-YY H:mm:ss UTC");
                    } else {
                        return data;
                    }
                }
            },
            {
                title: "Window Start",
                data: "start",
                width: "180px",
                render: function(data, type, row) {
                    if (type === "display") {
                        return moment(data).utc().format("M-D-YY H:mm:ss UTC")
                    } else {
                        return data;
                    }
                }
            },
            {
                title: "Window End",
                data: "end",
                width: "180px",
                render: function(data, type, row) {
                    if (type === "display") {
                        return moment(data).utc().format("M-D-YY H:mm:ss UTC");
                    } else {
                        return data;
                    }
                }
            }
        ],
        processing: true
    });

    // add row selection
    $("#logs table tbody").on("click", "tr", function () {
        if ($(this).attr("role") === "row") {

            // remove selected style and hide the details window
            $(this).siblings("tr.selected").removeClass("selected").each(function(_, tr) {
                const p_row = table.row(tr);
                if (p_row.child.isShown()) {
                    p_row.child.hide();
                }
            });

            // toggle the class and detail state
            $(this).toggleClass("selected");
            const row = table.row(this);
            if (row.child.isShown()) {
                row.child.hide();
            } else {
                if (row.child.length < 1) {
                    const details = detail(row.data());
                    row.child(details);
                }
                row.child.show();
            }

        }
    });

    return table;
}

$(document).ready(function() {

    // get parameters
    const pipeline = getParameterByName("pipeline");
    const activity = getParameterByName("activity");
    const datasets = getParameterByName("datasets");

    // build the table
    const table = build();

    // fix Edge bug where the page options aren't shown
    const pageLengthOptions = $("select[name='DataTables_Table_0_length']");
    const countOfPageLengthOptions = $(pageLengthOptions).children().length;
    if (countOfPageLengthOptions < 1) {
        $("<option />").val(10).text(10).appendTo(pageLengthOptions);
        $("<option />").val(25).text(25).appendTo(pageLengthOptions);
        $("<option />").val(50).text(50).appendTo(pageLengthOptions);
        $("<option />").val(100).text(100).appendTo(pageLengthOptions);
    }

    // query
    query(table, datasets);

});