
$(document).ready(() => {

    // create the datatables
    const instance_logs = $("#instance_logs").DataTable({
        ajax: {
            url: "/logs",
            dataSrc: ""
        },
        processing: true,
        columns: [
            { data: "index", width: "360px" },
            { data: "ts", width: "200px" },
            { data: "level", width: "50px" },
            { data: "msg" }
        ]
    });
    const associated_logs = $("#associated_logs").DataTable({
        ajax: {
            url: "/associated",
            dataSrc: ""
        },
        processing: true,
        columns: [
            { data: "ts", width: "200px" },
            { data: "level", width: "50px" },
            { data: "msg" }
        ]
    });

    // add selection
    $("#instance_logs").on("click", "tbody tr", function() {
        if (!$(this).hasClass("selected")) {
            instance_logs.$("tr.selected").removeClass("selected");
            $(this).addClass("selected");
            const row = instance_logs.row(this).data();
            associated_logs.ajax.url("/associated?apk=" + row.apk + "&ark=" + row.ark).load();
        }
    });

    // get the specified logs
    $("#get_instance_logs").click(() => {
        instance_logs.ajax.url("/logs?instanceId=" + $("#instanceId").val()).load();
    });

});