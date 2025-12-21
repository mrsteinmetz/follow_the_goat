<!DOCTYPE html>
<html lang="en" dir="ltr" data-nav-layout="vertical" data-theme-mode="dark" data-header-styles="dark" data-width="fullwidth" data-menu-styles="dark" data-page-style="flat" data-toggled="close" data-vertical-style="default">

    <head>

        <!-- Meta Data -->
        <meta charset="UTF-8">
        <meta name='viewport' content='width=device-width, initial-scale=1.0'>
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <meta name="Description" content="Follow The Goat - SOL Trading Dashboard">
        <meta name="Author" content="Follow The Goat">
    
        <!-- Title -->
        <title>Follow The Goat - SOL Dashboard</title>

        <!-- Favicon -->
        <link rel="icon" href="<?php echo $baseUrl; ?>/assets/images/brand-logos/favicon.ico" type="image/x-icon">

        <!-- Start::Styles -->
        <?php include __DIR__ . '/components/styles.php'; ?>
        <!-- End::Styles -->

        <!-- Custom Green Theme Override -->
        <style>
            :root {
                --primary-rgb: 16, 185, 129;  /* Emerald green */
                --secondary-rgb: 20, 184, 166; /* Teal accent */
            }
            
            /* Darker background for better contrast */
            [data-theme-mode="dark"] {
                --body-bg-rgb: 17, 17, 27;
                --body-bg-rgb2: 24, 24, 36;
            }
            
            /* Green success colors - make buttons more vibrant */
            .btn-primary {
                background: rgb(var(--primary-rgb)) !important;
                border-color: rgb(var(--primary-rgb)) !important;
            }
            .btn-primary:hover {
                background: rgb(5, 150, 105) !important;
                border-color: rgb(5, 150, 105) !important;
            }
        </style>

        <?php echo $styles ?? ''; ?>

    </head>

    <body class="">

        <div class="progress-top-bar"></div>

        <div class="page">

            <!-- Start::main-header -->
            <?php include __DIR__ . '/components/main-header.php'; ?>
            <!-- End::main-header -->

            <!-- Start::main-sidebar -->
            <?php include __DIR__ . '/components/main-sidebar.php'; ?>
            <!-- End::main-sidebar -->

            <!-- Start::app-content -->
            <div class="main-content app-content">
                <div class="container-fluid page-container main-body-container">

                    <?php echo $content ?? ''; ?>
                    
                </div>
            </div>
            <!-- End::app-content -->

            <!-- Start::main-modal -->
            <?php include __DIR__ . '/components/modal.php'; ?>
            <!-- End::main-modal -->

            <!-- Start::main-footer -->
            <?php include __DIR__ . '/components/footer.php'; ?>
            <!-- End::main-footer -->

        </div>

        <!-- Start::main-scripts -->
        <?php include __DIR__ . '/components/scripts.php'; ?>
        <!-- End::main-scripts -->

        <?php echo $scripts ?? ''; ?>

        <!-- Sticky JS -->
        <script src="<?php echo $baseUrl; ?>/assets/js/sticky.js"></script>

        <!-- Defaultmenu JS -->
        <script src="<?php echo $baseUrl; ?>/assets/js/defaultmenu.min.js"></script>

        <!-- Custom JS -->
        <script src="<?php echo $baseUrl; ?>/assets/js/custom.js"></script>

        <!-- Custom-Switcher JS -->
        <script src="<?php echo $baseUrl; ?>/assets/js/custom-switcher.min.js"></script>

    </body> 

</html>

