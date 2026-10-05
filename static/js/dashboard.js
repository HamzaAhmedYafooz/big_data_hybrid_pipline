/**
 * Big Data Pipeline — Executive Control Center
 * Modular JavaScript Engine with Full Bilingual (EN/AR) & RTL/LTR Support
 */

// ══════════════════════════════════════════════════════
//  INTERNATIONALIZATION (i18n) DICTIONARY
// ══════════════════════════════════════════════════════
const I18N = {
  en: {
    lang_btn: "🌐 العربية",
    brand_title: "Big Data Pipeline",
    brand_sub: "Al-Razi University",
    section_overview: "Overview",
    section_midterm: "Midterm",
    section_final: "Final Project",
    section_system: "System",
    nav_dashboard: "Dashboard",
    nav_ingest: "Data Ingestion",
    nav_indexes: "Indexes & Queries",
    nav_aggregations: "Aggregations",
    nav_matviews: "Materialized Views",
    nav_jobs: "Scheduled Jobs",
    nav_swagger: "Swagger API",
    api_label: "API: ",
    db_label: "MongoDB: ",
    checking: "Checking…",
    online: "Online",
    connected: "Connected",
    refresh_health: "🔄 Refresh Health",
    swagger_ui: "📚 Swagger UI",

    // Overview KPIs
    kpi_records: "Validated Records",
    kpi_records_sub: "in orders_validated",
    kpi_revenue: "Total Revenue",
    kpi_revenue_sub: "YER gross sales",
    kpi_days: "Daily Snapshots",
    kpi_days_sub: "daily_sales_summary MV",
    kpi_products: "Top Products",
    kpi_products_sub: "top_products_summary MV",

    // Overview Cards
    quick_actions: "🚀 Quick Actions",
    quick_actions_sub: "Run any pipeline step instantly",
    action_ingest: "📥 &nbsp;Run Ingest Pipeline (Small Sample)",
    action_indexes: "🔬 &nbsp;Create Indexes + Run Explain Benchmark",
    action_aggregations: "📊 &nbsp;View All 5 Aggregation Reports",
    action_matviews: "⚡ &nbsp;Refresh Materialized Views",
    action_jobs: "⏱️ &nbsp;View & Trigger Scheduled Jobs",
    system_health: "❤️ System Health",
    system_health_sub: "Live status from GET /health",
    refresh: "Refresh",
    health_placeholder: "// Click Refresh to check system health…",

    // Requirements Checklist
    req_title: "Project Requirements Checklist",
    req1_title: "REQ 1 · Midterm",
    req1_sub: "Ingestion + ELT Pipeline",
    req1_desc: "File Router → Batch/Spark → ELT → Validated Collection",
    req2_title: "REQ 2 · Final",
    req2_sub: "Indexes & Queries Explain",
    req2_desc: "5 indexes + COLLSCAN→IXSCAN benchmark on records",
    req3_title: "REQ 3 · Final",
    req3_sub: "Aggregation Reports",
    req3_desc: "5 aggregation pipelines: city, products, customers, period, status",
    req4_title: "REQ 4 · Final",
    req4_sub: "Materialized Views",
    req4_desc: "$merge incremental refresh: daily_sales + top_products",
    req5_title: "REQ 5 · Final",
    req5_sub: "Scheduled Jobs",
    req5_desc: "Background scheduler + MongoDB audit logs + manual trigger",
    req6_title: "REQ 6 · Final",
    req6_sub: "Unified FastAPI Server",
    req6_desc: "10 REST endpoints + Swagger UI + auto-evaluation ready",

    // Ingest Page
    ing_config: "⚙️ Pipeline Configuration",
    ing_title: "📥 Data Ingestion Pipeline",
    ing_subtitle: "Midterm REQ 1 · POST /ingest · File Router → ELT → MongoDB",
    dataset_mode: "Dataset Mode",
    opt_small: "Small Sample (for testing)",
    opt_huge: "Huge File (production ~912k rows)",
    btn_run_ingest: "▶ Run Ingest Pipeline",
    ing_desc: "The pipeline will: detect file size → choose engine (Python/Spark) → load raw data → run ELT (validate + quarantine) → store in orders_validated",
    pipeline_output: "Pipeline Output",
    ing_term_init: "// Click \"Run Ingest Pipeline\" to start…\n// The pipeline may take 10–60 seconds depending on dataset size.",
    kpi_raw: "Raw Loaded",
    kpi_raw_sub: "records from CSV",
    kpi_valid: "Validated",
    kpi_valid_sub: "passed ELT rules",
    kpi_quar: "Quarantined",
    kpi_quar_sub: "failed validation",

    // Indexes Page
    idx_card_title: "🗂️ Create Indexes",
    idx_card_sub: "Final REQ 2 · POST /indexes",
    btn_create_all: "▶ Create All",
    idx_desc: "Creates all required indexes on orders_validated including compound index idx_city_date.",
    idx_term_init: "// Click \"Create All\" to create indexes via POST /indexes…",
    explain_title: "🔬 Explain Benchmark",
    explain_sub: "COLLSCAN → IXSCAN comparison",
    btn_run_explain: "▶ Run Explain",
    stage: "Stage:",
    docs_examined: "Docs Examined:",
    time: "Time:",
    before_label: "Before (COLLSCAN)",
    after_label: "After (IXSCAN)",
    efficiency_gain: "Efficiency Gain:",
    click_explain: "Click \"Run Explain\"",

    // Queries Page
    queries_title: "🔍 Practical Queries",
    queries_sub: "GET /queries/{name} — 5 business queries with dynamic filtering & indexing",
    query_name: "Query Name",
    quick_queries_label: "Quick Run:",
    city_label: "City",
    city_optional: "City (optional)",
    customer_id_label: "Customer ID",
    status_label: "Order Status",
    min_amount_label: "Min Amount (YER)",
    payment_method_label: "Payment Method",
    opt_all_cities: "All Cities",
    opt_all_statuses: "All Statuses",
    start_date: "Start Date",
    end_date: "End Date",
    limit: "Limit",
    btn_run_query: "▶ Run Query",
    query_init: "Select a query and click \"Run Query\" to view records",

    // Aggregations Page
    agg_tab_city: "🏙️ Sales by City",
    agg_tab_products: "📦 Top Products",
    agg_tab_customers: "👑 Top Customers",
    agg_tab_period: "📅 Sales by Period",
    agg_tab_status: "📋 Orders by Status",
    btn_load_report: "▶ Load Report",
    agg_init: "Select a report and click \"Load Report\"",

    // Materialized Views Page
    mv_card_title: "⚡ Refresh Views",
    mv_card_sub: "Final REQ 4 · POST /refresh-mv",
    btn_mv_inc: "⚡ Incremental",
    btn_mv_full: "🔄 Full Rebuild",
    mv_desc: "Incremental: only processes new records since last checkpoint. Full Rebuild: drops and recreates the entire view from scratch.",
    mv_term_init: "// Click \"Incremental\" or \"Full Rebuild\" to refresh via POST /refresh-mv…",
    mv_status_title: "📊 View Status",
    mv_status_sub: "Materialized collections in MongoDB",
    btn_reload: "Reload",
    th_view: "View",
    th_records: "Records",
    th_mode: "Mode",
    th_status: "Status",
    daily_sales_title: "📅 Daily Sales Summary",
    daily_sales_sub: "GET /materialized/daily-sales · Precomputed daily revenue",
    btn_load_data: "Load Data",
    th_date: "Date",
    th_orders_count: "Orders Count",
    th_total_sales: "Total Sales (YER)",
    th_avg_order: "Avg Order Value",
    top_prod_title: "🏆 Top Products Summary",
    top_prod_sub: "GET /materialized/top-products · Precomputed product rankings",
    th_product: "Product",
    th_units_sold: "Units Sold",
    th_total_revenue: "Total Revenue",
    th_avg_price: "Avg Price",

    // Scheduled Jobs Page
    jobs_title: "⏱️ Registered Jobs — Final REQ 5",
    job1_title: "🔄 refresh_materialized_views",
    job1_desc: "Incrementally refreshes daily_sales_summary and top_products_summary via $merge pipeline",
    job2_title: "📊 generate_periodic_report",
    job2_desc: "Generates aggregated sales report and persists snapshot to MongoDB for historical tracking",
    btn_run_now: "▶ Run Now",
    tag_registered: "Registered",
    audit_title: "📋 Execution Audit Logs",
    audit_subtitle: "persisted in MongoDB: job_execution_logs",
    audit_history_title: "Audit History",
    audit_history_sub: "GET /jobs — recent execution records",
    btn_refresh_logs: "🔄 Refresh Logs",
    jobs_term_init: "// Click \"Run Now\" on a job or \"Refresh Logs\" to load history…",
    th_job_name: "Job Name",
    th_triggered_by: "Triggered By",
    th_started: "Started",
    th_duration: "Duration"
  },

  ar: {
    lang_btn: "🌐 English",
    brand_title: "خط بيانات التجارة الإلكترونية",
    brand_sub: "جامعة الرازي - البيانات الضخمة",
    section_overview: "نظرة عامة",
    section_midterm: "المشروع النصفي",
    section_final: "المشروع النهائي",
    section_system: "النظام",
    nav_dashboard: "لوحة التحكم الرئيسية",
    nav_ingest: "إدخال ومعالجة البيانات",
    nav_indexes: "الفهارس والاستعلامات",
    nav_aggregations: "تقارير التجميعات",
    nav_matviews: "العروض المادية",
    nav_jobs: "المهام المجدولة",
    nav_swagger: "توثيق Swagger API",
    api_label: "واجهة API: ",
    db_label: "قاعدة MongoDB: ",
    checking: "جارٍ الفحص…",
    online: "نشط ومتصل",
    connected: "متصلة بنجاح",
    refresh_health: "🔄 تحديث الحالة",
    swagger_ui: "📚 توثيق Swagger",

    // Overview KPIs
    kpi_records: "السجلات المعتمدة",
    kpi_records_sub: "في مجموعة orders_validated",
    kpi_revenue: "إجمالي المبيعات",
    kpi_revenue_sub: "ريال يمني (إجمالي الإيرادات)",
    kpi_days: "اللقطات اليومية",
    kpi_days_sub: "ملخص المبيعات اليومية MV",
    kpi_products: "أفضل المنتجات",
    kpi_products_sub: "ملخص أداء المنتجات MV",

    // Overview Cards
    quick_actions: "🚀 إجراءات سريعة فورية",
    quick_actions_sub: "تشغيل أي مرحلة من خط الأنابيب بضغطة واحدة",
    action_ingest: "📥 &nbsp;تشغيل خط الإدخال والـ ELT (عينة صغيرة)",
    action_indexes: "🔬 &nbsp;إنشاء الفهارس + تشغيل قياس Explain",
    action_aggregations: "📊 &nbsp;استعراض تقارير التجميعات الـ 5",
    action_matviews: "⚡ &nbsp;تحديث العروض المادية تزايدياً",
    action_jobs: "⏱️ &nbsp;استعراض وتشغيل المهام المجدولة",
    system_health: "❤️ صحة وجاهزية النظام",
    system_health_sub: "فحص الاتصال المباشر عبر GET /health",
    refresh: "تحديث",
    health_placeholder: "// اضغط تحديث لفحص الاتصال وحالة النظام…",

    // Requirements Checklist
    req_title: "قائمة متطلبات المشروع المكتملة",
    req1_title: "المطلب 1 · النصفي",
    req1_sub: "خط الإدخال والـ ELT",
    req1_desc: "موجه الملفات → المحرك بالدفعات/Spark → جودة البيانات والعزل → التخزين المعتمد",
    req2_title: "المطلب 2 · النهائي",
    req2_sub: "الفهارس ومقارنة Explain",
    req2_desc: "فهارس مفردة ومركبة + تحول المسار من COLLSCAN إلى IXSCAN وخفض الوثائق >99%",
    req3_title: "المطلب 3 · النهائي",
    req3_sub: "تقارير التجميعات",
    req3_desc: "5 خطوط تجميع: المبيعات بالمدينة، المنتجات، كبار العملاء، الفترات، الحالات",
    req4_title: "المطلب 4 · النهائي",
    req4_sub: "العروض المادية",
    req4_desc: "تحديث تزايدي ذكي عبر $merge لمجموعتي daily_sales و top_products",
    req5_title: "المطلب 5 · النهائي",
    req5_sub: "المهام المجدولة والتدقيق",
    req5_desc: "مجدول خلفي تلقائي + سجلات تدقيق بالملي ثانية + دعم التشغيل اليدوي",
    req6_title: "المطلب 6 · النهائي",
    req6_sub: "واجهة API الموحدة",
    req6_desc: "10 مسارات RESTful موثقة + Swagger UI + جاهزية التقييم الآلي",

    // Ingest Page
    ing_config: "⚙️ إعدادات خط أنابيب البيانات",
    ing_title: "📥 خط إدخال ومعالجة البيانات (ELT)",
    ing_subtitle: "المشروع النصفي · POST /ingest · File Router → ELT → MongoDB",
    dataset_mode: "حجم مجموعة البيانات",
    opt_small: "عينة صغيرة للاختبار السريع (20 ألف سجل)",
    opt_huge: "ملف عملاق للإنتاج الفعلي (~912 ألف سجل)",
    btn_run_ingest: "▶ تشغيل خط الإدخال والـ ELT",
    ing_desc: "يقوم الخط بـ: فحص الحجم → اختيار المحرك (بايثون/Spark) → تحميل الخام → تطبيق 12 قاعدة عزل و10 تصحيح → الحفظ في orders_validated",
    pipeline_output: "سجل مخرجات المعالجة",
    ing_term_init: "// اضغط \"تشغيل خط الإدخال والـ ELT\" للبدء…\n// تستغرق المعالجة بضع ثوانٍ بحسب حجم الملف.",
    kpi_raw: "البيانات المقروءة",
    kpi_raw_sub: "سجل من ملف CSV",
    kpi_valid: "السجلات المعتمدة",
    kpi_valid_sub: "اجتازت قواعد الجودة (Validated)",
    kpi_quar: "السجلات المعزولة",
    kpi_quar_sub: "معزولة بسبب أخطاء (Quarantine)",

    // Indexes Page
    idx_card_title: "🗂️ إنشاء وتفعيل الفهارس",
    idx_card_sub: "المشروع النهائي · POST /indexes",
    btn_create_all: "▶ إنشاء وتفعيل الكل",
    idx_desc: "إنشاء كافة الفهارس المطلوبة على orders_validated متضمنة الفهرس المركب idx_city_date.",
    idx_term_init: "// اضغط \"إنشاء وتفعيل الكل\" لبناء الفهارس في قاعدة البيانات…",
    explain_title: "🔬 قياس أداء الاستعلامات (Explain Benchmark)",
    explain_sub: "مقارنة حية للتحول من COLLSCAN إلى IXSCAN",
    btn_run_explain: "▶ تشغيل اختبار Explain",
    stage: "مرحلة الفحص:",
    docs_examined: "الوثائق المفحوصة:",
    time: "زمن التنفيذ:",
    before_label: "قبل الفهرسة (مسح شامل COLLSCAN)",
    after_label: "بعد الفهرسة (عبر الفهرس IXSCAN)",
    efficiency_gain: "نسبة التحسن في الكفاءة:",
    click_explain: "اضغط \"تشغيل اختبار Explain\"",

    // Queries Page
    queries_title: "🔍 الاستعلامات العملية الخمسة",
    queries_sub: "GET /queries/{name} — تنفيذ استعلامات الأعمال مع الفلترة الديناميكية والفهارس",
    query_name: "اسم الاستعلام",
    quick_queries_label: "تشغيل سريع:",
    city_label: "المدينة",
    city_optional: "المدينة (اختياري)",
    customer_id_label: "معرف العميل",
    status_label: "حالة الطلب",
    min_amount_label: "الحد الأدنى للمبلغ (ريال)",
    payment_method_label: "وسيلة الدفع",
    opt_all_cities: "جميع المدن",
    opt_all_statuses: "كافة الحالات",
    start_date: "تاريخ البدء",
    end_date: "تاريخ النهاية",
    limit: "حد السجلات",
    btn_run_query: "▶ تنفيذ الاستعلام",
    query_init: "قم باختيار استعلام ثم اضغط \"تنفيذ الاستعلام\" لعرض النتائج",

    // Aggregations Page
    agg_tab_city: "🏙️ المبيعات حسب المدينة",
    agg_tab_products: "📦 أفضل المنتجات مبيعاً",
    agg_tab_customers: "👑 أكثر العملاء إنفاقاً",
    agg_tab_period: "📅 المبيعات عبر الفترات",
    agg_tab_status: "📋 الطلبات حسب الحالة",
    btn_load_report: "▶ تشغيل التقرير",
    agg_init: "اختر نوع التقرير ثم اضغط \"تشغيل التقرير\"",

    // Materialized Views Page
    mv_card_title: "⚡ تحديث العروض المادية",
    mv_card_sub: "المشروع النهائي · POST /refresh-mv",
    btn_mv_inc: "⚡ تحديث تزايدي (Incremental)",
    btn_mv_full: "🔄 إعادة بناء شاملة (Full Rebuild)",
    mv_desc: "التحديث التزايدي: يعالج البيانات الجديدة فقط منذ آخر نقطة توقف. التحديث الشامل: يعيد احتساب العرض كاملاً من الصفر.",
    mv_term_init: "// اضغط على زر التحديث لتشغيل المعالجة التزايدية عبر POST /refresh-mv…",
    mv_status_title: "📊 حالة العروض المادية",
    mv_status_sub: "المجموعات المجمعة المخزنة في MongoDB",
    btn_reload: "تحديث الحالة",
    th_view: "اسم العرض المادي",
    th_records: "عدد السجلات",
    th_mode: "النمط",
    th_status: "الحالة",
    daily_sales_title: "📅 ملخص المبيعات اليومية",
    daily_sales_sub: "GET /materialized/daily-sales · بيانات مجمعة مسبقاً لكل يوم",
    btn_load_data: "جلب البيانات",
    th_date: "التاريخ",
    th_orders_count: "عدد الطلبات",
    th_total_sales: "إجمالي المبيعات (ريال)",
    th_avg_order: "متوسط قيمة الطلب",
    top_prod_title: "🏆 ملخص أفضل المنتجات",
    top_prod_sub: "GET /materialized/top-products · تصنيف المنتجات الأكثر إيراداً",
    th_product: "المنتج",
    th_units_sold: "الوحدات المباعة",
    th_total_revenue: "إجمالي الإيرادات",
    th_avg_price: "متوسط السعر",

    // Scheduled Jobs Page
    jobs_title: "⏱️ المهام المجدولة — المطلب الخامس",
    job1_title: "🔄 تحديث العروض المادية (refresh_materialized_views)",
    job1_desc: "تحديث تزايدي دوري للعروض المادية عبر خط أنابيب $merge كل 10 دقائق",
    job2_title: "📊 إنشاء التقرير الدوري (generate_periodic_report)",
    job2_desc: "توليد ملخص إحصائي شامل وحفظ اللقطة في مجموعة periodic_reports للمتابعة التاريخية",
    btn_run_now: "▶ تشغيل فوري",
    tag_registered: "مسجلة ونشطة",
    audit_title: "📋 سجلات تدقيق تنفيذ المهام",
    audit_subtitle: "موثقة بالكامل في مجموعة job_execution_logs",
    audit_history_title: "سجل العمليات السابقة",
    audit_history_sub: "GET /jobs — استعراض أحدث عمليات التنفيذ وحالات النجاح والمدة",
    btn_refresh_logs: "🔄 تحديث السجلات",
    jobs_term_init: "// اضغط \"تشغيل فوري\" لأي مهمة أو \"تحديث السجلات\" لعرض التوثيق…",
    th_job_name: "اسم المهمة",
    th_triggered_by: "طريقة التشغيل",
    th_started: "وقت البدء",
    th_duration: "المدة"
  }
};

let currentLang = localStorage.getItem('dashboard_lang') || 'ar'; // Default to Arabic as requested

// ══════════════════════════════════════════════════════
//  LANGUAGE TOGGLE & LOCALIZATION
// ══════════════════════════════════════════════════════
function toggleLanguage() {
  currentLang = currentLang === 'ar' ? 'en' : 'ar';
  localStorage.setItem('dashboard_lang', currentLang);
  applyLanguage(currentLang);
}

function applyLanguage(lang) {
  const dict = I18N[lang] || I18N.en;
  const isRTL = lang === 'ar';

  document.documentElement.setAttribute('lang', lang);
  document.documentElement.setAttribute('dir', isRTL ? 'rtl' : 'ltr');

  // Update elements with data-i18n
  document.querySelectorAll('[data-i18n]').forEach(el => {
    const key = el.getAttribute('data-i18n');
    if (dict[key]) {
      el.innerHTML = dict[key];
    }
  });

  // Update elements with data-i18n-placeholder
  document.querySelectorAll('[data-i18n-placeholder]').forEach(el => {
    const key = el.getAttribute('data-i18n-placeholder');
    if (dict[key]) {
      el.setAttribute('placeholder', dict[key]);
    }
  });

  // Update Language switcher button
  const langBtn = document.getElementById('lang-toggle-btn');
  if (langBtn) {
    langBtn.textContent = dict.lang_btn;
  }

  // Update dynamic page title
  const activePage = pages.find(p => document.getElementById('page-' + p)?.classList.contains('active')) || 'overview';
  updatePageTitle(activePage);

  // Update Aggregations header
  if (typeof aggMeta !== 'undefined' && aggMeta[currentAgg]) {
    document.getElementById('agg-title').textContent = isRTL ? aggMeta[currentAgg].title_ar : aggMeta[currentAgg].title_en;
  }
}

// ══════════════════════════════════════════════════════
//  NAVIGATION
// ══════════════════════════════════════════════════════
const pages = ['overview', 'ingest', 'indexes', 'aggregations', 'matviews', 'jobs'];

const pageTitles = {
  overview: {
    en: 'Dashboard <span>Overview</span>',
    ar: 'لوحة التحكم <span>نظرة عامة</span>'
  },
  ingest: {
    en: 'Data Ingestion <span>Midterm REQ 1 · POST /ingest</span>',
    ar: 'إدخال البيانات <span>المشروع النصفي · POST /ingest</span>'
  },
  indexes: {
    en: 'Indexes & Queries <span>Final REQ 2</span>',
    ar: 'الفهارس والاستعلامات <span>المشروع النهائي · REQ 2</span>'
  },
  aggregations: {
    en: 'Aggregation Reports <span>Final REQ 3</span>',
    ar: 'تقارير التجميعات <span>المشروع النهائي · REQ 3</span>'
  },
  matviews: {
    en: 'Materialized Views <span>Final REQ 4</span>',
    ar: 'العروض المادية <span>المشروع النهائي · REQ 4</span>'
  },
  jobs: {
    en: 'Scheduled Jobs <span>Final REQ 5</span>',
    ar: 'المهام المجدولة <span>المشروع النهائي · REQ 5</span>'
  }
};

let currentAgg = 'sales_by_city';

function updatePageTitle(id) {
  const el = document.getElementById('page-title');
  if (el) {
    const titles = pageTitles[id];
    el.innerHTML = titles ? (titles[currentLang] || titles.en) : id;
  }
}

function nav(id, el) {
  pages.forEach(p => {
    const pageEl = document.getElementById('page-' + p);
    if (pageEl) pageEl.classList.remove('active');
  });

  const target = document.getElementById('page-' + id);
  if (target) target.classList.add('active');

  updatePageTitle(id);

  document.querySelectorAll('.sb-item').forEach(i => i.classList.remove('active'));
  if (el) el.classList.add('active');

  // Auto-load triggers
  if (id === 'overview') {
    checkHealth();
    loadOverviewKPIs();
  }
  if (id === 'jobs') {
    loadJobs();
  }
}

// ══════════════════════════════════════════════════════
//  NOTIFICATION TOAST
// ══════════════════════════════════════════════════════
let toastTimer;
function toast(msg, type = 'ok') {
  const icons = { ok: '✅', err: '❌', warn: '⚠️', info: 'ℹ️' };
  const iconEl = document.getElementById('toast-icon');
  const msgEl = document.getElementById('toast-msg');
  const toastEl = document.getElementById('toast');

  if (iconEl) iconEl.textContent = icons[type] || '✅';
  if (msgEl) msgEl.textContent = msg;
  if (toastEl) {
    toastEl.classList.add('show');
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => toastEl.classList.remove('show'), 4000);
  }
}

// ══════════════════════════════════════════════════════
//  TERMINAL HELPERS
// ══════════════════════════════════════════════════════
function termLog(id, msg, cls = '') {
  const el = document.getElementById(id);
  if (!el) return;
  const line = document.createElement('div');
  if (cls) line.className = cls;
  line.textContent = msg;
  if (el.textContent.startsWith('//')) el.textContent = '';
  el.appendChild(line);
  el.scrollTop = el.scrollHeight;
}

function termClear(id, msg = '') {
  const el = document.getElementById(id);
  if (el) el.textContent = msg;
}

function termJSON(id, obj) {
  termLog(id, JSON.stringify(obj, null, 2), 't-muted');
}

// ══════════════════════════════════════════════════════
//  HEALTH CHECK
// ══════════════════════════════════════════════════════
async function checkHealth() {
  termClear('health-terminal', '');
  termLog('health-terminal', '▶ GET /health', 't-info');
  const isAr = currentLang === 'ar';
  try {
    const r = await fetch('/health');
    const d = await r.json();
    const ok = d.status === 'healthy';
    termLog('health-terminal', `Status   : ${d.status}`, ok ? 't-ok' : 't-err');
    termLog('health-terminal', `Database : ${d.database}`, ok ? 't-ok' : 't-warn');
    termLog('health-terminal', `Version  : ${d.version || '2.0.0'}`);
    termLog('health-terminal', `Timestamp: ${d.timestamp}`);

    document.getElementById('sb-api-status').textContent = ok ? (isAr ? 'متصل' : 'Online') : (isAr ? 'خطأ' : 'Error');
    document.getElementById('sb-db-status').textContent = d.database === 'connected' ? (isAr ? 'متصلة' : 'Connected') : d.database;
    const dbDot = document.getElementById('sb-dot-db');
    if (dbDot) dbDot.className = 'dot ' + (d.database === 'connected' ? 'dot-green' : 'dot-red');
    toast(isAr ? 'النظام وقاعدة البيانات جاهزة ومتصلة' : 'Health check OK — MongoDB connected', 'ok');
  } catch (e) {
    termLog('health-terminal', 'Error: ' + e, 't-err');
    document.getElementById('sb-api-status').textContent = isAr ? 'غير متصل' : 'Offline';
    document.getElementById('sb-dot-api').className = 'dot dot-red';
    toast(isAr ? 'تعذر الاتصال بالخادم' : 'API unreachable', 'err');
  }
}

// ══════════════════════════════════════════════════════
//  OVERVIEW KPIs
// ══════════════════════════════════════════════════════
async function loadOverviewKPIs() {
  try {
    const r = await fetch('/aggregations/sales_by_city?limit=50');
    const d = await r.json();
    if (d.results && d.results.length) {
      const total = d.results.reduce((a, b) => a + (b.total_sales || 0), 0);
      const totalOrders = d.results.reduce((a, b) => a + (b.orders_count || 0), 0);
      document.getElementById('kpi-records').textContent = totalOrders.toLocaleString();
      document.getElementById('kpi-revenue').textContent = (total / 1e9).toFixed(2) + ' B';
    }
  } catch (_) {}

  try {
    const r2 = await fetch('/materialized/daily-sales?limit=365');
    const d2 = await r2.json();
    if (d2.results) {
      const isAr = currentLang === 'ar';
      document.getElementById('kpi-days').textContent = d2.results.length + (isAr ? ' يوم' : ' Days');
    }
  } catch (_) {}

  try {
    const r3 = await fetch('/materialized/top-products?limit=50');
    const d3 = await r3.json();
    if (d3.results) {
      const isAr = currentLang === 'ar';
      document.getElementById('kpi-products').textContent = d3.results.length + (isAr ? ' منتج' : ' SKUs');
    }
  } catch (_) {}
}

// ══════════════════════════════════════════════════════
//  DATA INGESTION (ELT)
// ══════════════════════════════════════════════════════
async function runIngest() {
  const mode = document.getElementById('ingest-mode').value;
  const btn = document.getElementById('btn-ingest');
  btn.disabled = true;
  btn.innerHTML = '<span class="spin"></span> ' + (currentLang === 'ar' ? 'جارٍ التشغيل…' : 'Running…');
  termClear('ingest-terminal', '');
  termLog('ingest-terminal', '▶ POST /ingest — mode: ' + mode, 't-info');
  termLog('ingest-terminal', currentLang === 'ar' ? '  قد تستغرق العملية من 5 إلى 30 ثانية بحسب حجم البيانات…' : '  This may take 10-120 seconds…', 't-muted');

  const body = mode === 'huge' ? { file_path: null } : {};
  try {
    const r = await fetch('/ingest', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const d = await r.json();
    if (d.status === 'success') {
      termLog('ingest-terminal', '✓ Status        : ' + d.status, 't-ok');
      termLog('ingest-terminal', '  Run ID        : ' + d.run_id);
      termLog('ingest-terminal', '  Engine Used   : ' + d.engine_used, 't-info');
      termLog('ingest-terminal', '  Raw Loaded    : ' + (d.raw_records_loaded || 0).toLocaleString(), 't-ok');

      if (d.metrics) {
        const m = d.metrics;
        // Robust fallback checking for valid / validated count
        const validVal = m.validated_count != null ? m.validated_count : (m.inserted_count != null ? m.inserted_count : ((m.valid_count || 0) + (m.corrected_count || 0)));
        const durVal = m.total_seconds != null ? m.total_seconds : (m.elapsed_seconds != null ? m.elapsed_seconds : '—');

        termLog('ingest-terminal', '  Validated     : ' + (validVal != null ? Number(validVal).toLocaleString() : '—'), 't-ok');
        termLog('ingest-terminal', '  Quarantined   : ' + (m.quarantine_count != null ? Number(m.quarantine_count).toLocaleString() : '—'), 't-warn');
        termLog('ingest-terminal', '  Duration      : ' + durVal + 's');

        document.getElementById('ing-raw').textContent = (d.raw_records_loaded || 0).toLocaleString();
        document.getElementById('ing-validated').textContent = validVal != null ? Number(validVal).toLocaleString() : '—';
        document.getElementById('ing-quarantined').textContent = m.quarantine_count != null ? Number(m.quarantine_count).toLocaleString() : '—';
      }

      toast(currentLang === 'ar' ? `اكتمل الإدخال — تم معالجة ${d.raw_records_loaded.toLocaleString()} سجل` : `Ingest complete — ${d.raw_records_loaded} records loaded`, 'ok');
      loadOverviewKPIs();
    } else {
      termLog('ingest-terminal', 'Error: ' + JSON.stringify(d), 't-err');
      toast(currentLang === 'ar' ? 'فشلت عملية الإدخال' : 'Ingest failed', 'err');
    }
  } catch (e) {
    termLog('ingest-terminal', 'Error: ' + e, 't-err');
    toast(currentLang === 'ar' ? 'خطأ في الإدخال: ' + e : 'Ingest error: ' + e, 'err');
  }

  btn.disabled = false;
  btn.innerHTML = I18N[currentLang].btn_run_ingest;
}

// ══════════════════════════════════════════════════════
//  INDEXES & EXPLAIN BENCHMARK
// ══════════════════════════════════════════════════════
async function createIndexes() {
  const btn = document.getElementById('btn-create-idx');
  btn.disabled = true;
  btn.innerHTML = '<span class="spin"></span> ' + (currentLang === 'ar' ? 'جارٍ الإنشاء…' : 'Creating…');
  termClear('idx-terminal', '');
  termLog('idx-terminal', '▶ POST /indexes', 't-info');

  try {
    const r = await fetch('/indexes', { method: 'POST' });
    const d = await r.json();
    termLog('idx-terminal', '✓ Status  : ' + d.status, 't-ok');
    termLog('idx-terminal', '  Message : ' + d.message, 't-ok');
    if (d.indexes_created) {
      d.indexes_created.forEach(idx => {
        const name = typeof idx === 'string' ? idx : idx.name;
        const type = idx.type ? ` (${idx.type})` : '';
        termLog('idx-terminal', '  Index   : ' + name + type, 't-info');
      });
    }
    toast(currentLang === 'ar' ? `تم تفعيل ${ (d.indexes_created || []).length } فهارس بنجاح` : `Indexes created: ${ (d.indexes_created || []).length }`, 'ok');
  } catch (e) {
    termLog('idx-terminal', 'Error: ' + e, 't-err');
    toast(currentLang === 'ar' ? 'فشل إنشاء الفهارس' : 'Index creation failed', 'err');
  }

  btn.disabled = false;
  btn.innerHTML = I18N[currentLang].btn_create_all;
}

async function runExplain() {
  const btn = document.getElementById('btn-explain');
  btn.disabled = true;
  btn.innerHTML = '<span class="spin"></span> ' + (currentLang === 'ar' ? 'جارٍ القياس…' : 'Running…');
  document.getElementById('gain-pill').textContent = currentLang === 'ar' ? 'جارٍ الفحص والمقارنة…' : 'Running explain…';

  try {
    const r = await fetch('/indexes/explain');
    const d = await r.json();
    if (d.benchmark_report && d.benchmark_report.length > 0) {
      const rep = d.benchmark_report.find(item => item.query_name === 'city_date_range') || d.benchmark_report[0];

      document.getElementById('e-b-stage').textContent = (rep.before.stages || []).join(' → ');
      document.getElementById('e-b-docs').textContent = (rep.before.docs_examined || 0).toLocaleString();
      document.getElementById('e-b-time').textContent = rep.before.time_ms + ' ms';

      document.getElementById('e-a-stage').textContent = (rep.after.stages || []).join(' → ');
      document.getElementById('e-a-docs').textContent = (rep.after.docs_examined || 0).toLocaleString();
      document.getElementById('e-a-time').textContent = rep.after.time_ms + ' ms';

      const gain = rep.efficiency_gain || {};
      const pct = gain.docs_scanned_reduction_pct || gain.time_reduction_pct || '—';
      document.getElementById('gain-pill').textContent = '📉 ' + pct + (currentLang === 'ar' ? ' انخفاض في الفحص' : ' Reduction');
      toast(currentLang === 'ar' ? 'تم اكتمال اختبار Explain بنجاح' : 'Explain benchmark complete', 'ok');
    } else {
      document.getElementById('gain-pill').textContent = currentLang === 'ar' ? 'لا توجد بيانات' : 'No data';
    }
  } catch (e) {
    document.getElementById('gain-pill').textContent = 'Error: ' + e;
    toast(currentLang === 'ar' ? 'فشل اختبار Explain' : 'Explain failed', 'err');
  }

  btn.disabled = false;
  btn.innerHTML = I18N[currentLang].btn_run_explain;
}

// ══════════════════════════════════════════════════════
//  PRACTICAL QUERIES (DYNAMIC ENGINE)
// ══════════════════════════════════════════════════════
function onQuerySelectChange() {
  const select = document.getElementById('query-name');
  if (!select) return;
  const name = select.value;

  // Update chip active status
  document.querySelectorAll('.query-chip').forEach(ch => {
    ch.classList.toggle('active', ch.id === `chip-${name}`);
  });

  const wrapCity = document.getElementById('wrap-city');
  const wrapCustomer = document.getElementById('wrap-customer');
  const wrapStatus = document.getElementById('wrap-status');
  const wrapAmount = document.getElementById('wrap-amount');
  const wrapPayment = document.getElementById('wrap-payment');
  const wrapStart = document.getElementById('wrap-start');
  const wrapEnd = document.getElementById('wrap-end');

  // Hide all dynamic inputs initially
  [wrapCity, wrapCustomer, wrapStatus, wrapAmount, wrapPayment, wrapStart, wrapEnd].forEach(el => {
    if (el) el.style.display = 'none';
  });

  // Display only controls relevant to the selected query
  if (name === 'city_date_range') {
    if (wrapCity) wrapCity.style.display = 'flex';
    if (wrapStart) wrapStart.style.display = 'flex';
    if (wrapEnd) wrapEnd.style.display = 'flex';
  } else if (name === 'customer_orders') {
    if (wrapCustomer) wrapCustomer.style.display = 'flex';
  } else if (name === 'city_orders') {
    if (wrapCity) wrapCity.style.display = 'flex';
  } else if (name === 'status_high_value') {
    if (wrapStatus) wrapStatus.style.display = 'flex';
    if (wrapAmount) wrapAmount.style.display = 'flex';
  } else if (name === 'payment_method_orders') {
    if (wrapPayment) wrapPayment.style.display = 'flex';
  }
}

function selectQuickQuery(name) {
  const select = document.getElementById('query-name');
  if (select) {
    select.value = name;
    onQuerySelectChange();
    runQuery();
  }
}

async function runQuery() {
  const nameSelect = document.getElementById('query-name');
  if (!nameSelect) return;
  const name = nameSelect.value;
  const lim = document.getElementById('q-limit')?.value || 20;
  const btn = document.getElementById('btn-query');

  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spin"></span>';
  }

  let url = `/queries/${name}?limit=${lim}`;

  if (name === 'city_date_range') {
    const city = document.getElementById('q-city')?.value;
    const start = document.getElementById('q-start')?.value.trim();
    const end = document.getElementById('q-end')?.value.trim();
    if (city && city !== 'all') url += `&city=${encodeURIComponent(city)}`;
    if (start) url += `&start_date=${encodeURIComponent(start)}`;
    if (end) url += `&end_date=${encodeURIComponent(end)}`;
  } else if (name === 'customer_orders') {
    const cid = document.getElementById('q-customer')?.value.trim();
    if (cid) url += `&customer_id=${encodeURIComponent(cid)}`;
  } else if (name === 'city_orders') {
    const city = document.getElementById('q-city')?.value;
    if (city && city !== 'all') url += `&city=${encodeURIComponent(city)}`;
  } else if (name === 'status_high_value') {
    const status = document.getElementById('q-status')?.value;
    const minAmt = document.getElementById('q-min-amount')?.value;
    if (status && status !== 'all') url += `&status=${encodeURIComponent(status)}`;
    if (minAmt) url += `&min_amount=${encodeURIComponent(minAmt)}`;
  } else if (name === 'payment_method_orders') {
    const method = document.getElementById('q-payment')?.value;
    if (method && method !== 'all') url += `&payment_method=${encodeURIComponent(method)}`;
  }

  try {
    const r = await fetch(url);
    const d = await r.json();
    const rows = d.results || d.documents || [];

    // Show and populate Meta Bar
    const metaBar = document.getElementById('query-meta-bar');
    if (metaBar) metaBar.style.display = 'flex';

    const isAr = currentLang === 'ar';
    const metaCount = document.getElementById('q-meta-count');
    const metaTime = document.getElementById('q-meta-time');
    const metaIndex = document.getElementById('q-meta-index');
    const metaFilter = document.getElementById('q-meta-filter');

    if (metaCount) metaCount.textContent = isAr ? `✓ تم جلب ${rows.length} سجل` : `✓ ${rows.length} records fetched`;
    if (metaTime) metaTime.textContent = `⏱️ ${d.execution_time_seconds || '0.000'}s`;
    if (metaIndex) metaIndex.textContent = `🗂️ ${d.target_index || '—'} (${d.index_type || 'Index'})`;
    if (metaFilter) metaFilter.textContent = JSON.stringify(d.filter_applied || {});

    const thead = document.getElementById('query-thead');
    const tbody = document.getElementById('query-tbody');

    if (rows.length === 0) {
      if (thead) thead.innerHTML = `<tr><td colspan="8" style="color:var(--muted);text-align:center;padding:24px">${isAr ? 'لا توجد نتائج مطابقة لمعايير البحث' : 'No matching records found'}</td></tr>`;
      if (tbody) tbody.innerHTML = '';
    } else {
      if (thead) {
        thead.innerHTML = `
          <tr>
            <th>#</th>
            <th>${isAr ? 'معرف الطلب' : 'Order ID'}</th>
            <th>${isAr ? 'العميل' : 'Customer'}</th>
            <th>${isAr ? 'المدينة' : 'City'}</th>
            <th>${isAr ? 'التاريخ' : 'Date'}</th>
            <th>${isAr ? 'الحالة' : 'Status'}</th>
            <th>${isAr ? 'المبلغ الإجمالي' : 'Total Amount'}</th>
            <th>${isAr ? 'طريقة الدفع' : 'Payment Method'}</th>
          </tr>
        `;
      }

      if (tbody) {
        tbody.innerHTML = rows.map((row, i) => {
          let statusTag = 'tag-cyan';
          const st = String(row.status || '');
          if (st.includes('تم التسليم') || st.toLowerCase().includes('delivered')) statusTag = 'tag-green';
          else if (st.includes('مؤكد') || st.toLowerCase().includes('confirmed')) statusTag = 'tag-blue';
          else if (st.includes('شحن') || st.includes('انتظار') || st.toLowerCase().includes('pending')) statusTag = 'tag-amber';
          else if (st.includes('ملغي') || st.includes('مرتجع') || st.toLowerCase().includes('cancel')) statusTag = 'tag-red';

          const amt = Number(row.total_amount || 0);
          const amtStr = !isNaN(amt) && amt > 0 ? amt.toLocaleString() + ' YER' : String(row.total_amount || '—');

          return `
            <tr>
              <td class="mono" style="color:var(--muted)">${i + 1}</td>
              <td><strong class="mono" style="color:var(--cyan)">${row.order_id || '—'}</strong></td>
              <td>
                <div style="font-weight:600">${row.customer_name || '—'}</div>
                <div class="mono" style="font-size:10px;color:var(--muted)">${row.customer_id || '—'}</div>
              </td>
              <td><span class="tag" style="background:rgba(255,255,255,0.05)">${row.city || '—'}</span></td>
              <td class="mono" style="font-size:11px;color:var(--muted)">${String(row.order_date || '—').replace('T', ' ')}</td>
              <td><span class="tag ${statusTag}">${row.status || '—'}</span></td>
              <td><strong class="mono">${amtStr}</strong></td>
              <td style="font-size:12px;color:var(--muted)">${row.payment_method || '—'}</td>
            </tr>
          `;
        }).join('');
      }
    }
    toast(isAr ? `تم جلب ${rows.length} سجل بنجاح` : `Query returned ${rows.length} records`, 'ok');
  } catch (e) {
    toast((currentLang === 'ar' ? 'فشل تنفيذ الاستعلام: ' : 'Query failed: ') + e, 'err');
  }

  if (btn) {
    btn.disabled = false;
    btn.innerHTML = I18N[currentLang]?.btn_run_query || '▶ Run Query';
  }
}

// ══════════════════════════════════════════════════════
//  AGGREGATIONS
// ══════════════════════════════════════════════════════
const aggMeta = {
  sales_by_city: {
    title_en: '🏙️ Sales by City',
    title_ar: '🏙️ إجمالي المبيعات حسب المدينة',
    sub_en: 'GET /aggregations/sales_by_city — $group by city',
    sub_ar: 'تجميع المبيعات وعدد الطلبات ومتوسط السلة لكل مدينة ($group)'
  },
  top_products: {
    title_en: '📦 Top Products',
    title_ar: '📦 المنتجات الأكثر مبيعاً وإيراداً',
    sub_en: 'GET /aggregations/top_products — from materialized view',
    sub_ar: 'تفكيك السلة وعرض أكثر المنتجات طلباً وإيراداً'
  },
  top_customers: {
    title_en: '👑 Top Customers',
    title_ar: '👑 كبار العملاء الأكثر إنفاقاً',
    sub_en: 'GET /aggregations/top_customers — $group by customer_id',
    sub_ar: 'تحديد كبار العملاء وبرامج الولاء ($group by customer_id)'
  },
  sales_by_period: {
    title_en: '📅 Sales by Period',
    title_ar: '📅 نمو المبيعات عبر الفترات الزمنية',
    sub_en: 'GET /aggregations/sales_by_period — $group by date',
    sub_ar: 'تتبع الإيرادات التاريخية مع الترتيب الزمني'
  },
  orders_by_status: {
    title_en: '📋 Orders by Status',
    title_ar: '📋 توزيع الطلبات بحسب حالاتها',
    sub_en: 'GET /aggregations/orders_by_status — $group by status',
    sub_ar: 'نسب الطلبات المكتملة، الملغية، وقيد التوصيل'
  }
};

function loadAgg(name, el) {
  currentAgg = name;
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  if (el) el.classList.add('active');

  const meta = aggMeta[name];
  const isAr = currentLang === 'ar';
  document.getElementById('agg-title').textContent = meta ? (isAr ? meta.title_ar : meta.title_en) : name;
  document.getElementById('agg-sub').textContent = meta ? (isAr ? meta.sub_ar : meta.sub_en) : '';
  document.getElementById('agg-thead').innerHTML = `<tr><td colspan="5" style="color:var(--muted);text-align:center;padding:16px">${I18N[currentLang].agg_init}</td></tr>`;
  document.getElementById('agg-tbody').innerHTML = '';
  document.getElementById('agg-meta').textContent = '';
  document.getElementById('agg-stats').innerHTML = '';
}

function reloadAgg() {
  runAgg(currentAgg);
}

async function runAgg(name) {
  const lim = document.getElementById('agg-limit').value || 20;
  const btn = document.getElementById('btn-agg');
  btn.disabled = true;
  btn.innerHTML = '<span class="spin"></span> ' + (currentLang === 'ar' ? 'جارٍ التحميل…' : 'Loading…');

  try {
    const r = await fetch(`/aggregations/${name}?limit=${lim}`);
    const d = await r.json();
    const rows = d.results || [];
    const isAr = currentLang === 'ar';

    document.getElementById('agg-meta').textContent = isAr
      ? `عدد السجلات: ${rows.length} · زمن التنفيذ: ${d.execution_time_seconds || '—'} ثانية · المصدر: ${d.source || 'MongoDB Aggregation'}`
      : `${rows.length} records · ${d.execution_time_seconds || '—'}s · Source: ${d.source || 'aggregation'}`;

    const stats = document.getElementById('agg-stats');
    stats.innerHTML = `
      <span class="tag tag-cyan">${isAr ? 'الإجمالي: ' : 'Total: '} ${rows.length} ${isAr ? 'سجل' : 'rows'}</span>
      <span class="tag tag-purple">${isAr ? 'المدة: ' : 'Time: '} ${d.execution_time_seconds || '—'}s</span>
    `;

    if (rows.length === 0) {
      document.getElementById('agg-thead').innerHTML = `<tr><td colspan="5" style="color:var(--muted);text-align:center;padding:16px">${isAr ? 'لا توجد بيانات متاحة' : 'No results returned'}</td></tr>`;
      document.getElementById('agg-tbody').innerHTML = '';
    } else {
      const allCols = Object.keys(rows[0]);
      document.getElementById('agg-thead').innerHTML = '<tr>' + allCols.map(c => `<th>${c}</th>`).join('') + '</tr>';

      document.getElementById('agg-tbody').innerHTML = rows.map(row => {
        const numKeys = allCols.filter(k => typeof row[k] === 'number');
        const maxVal = rows[0][numKeys[0]] || 1;
        const thisVal = row[numKeys[0]] || 0;
        const pct = Math.round((thisVal / maxVal) * 100);
        const barCol = numKeys[0];

        return '<tr>' + allCols.map(c => {
          let val = row[c];
          if (typeof val === 'number') val = val.toLocaleString();
          if (val === null || val === undefined) val = '—';
          let cell = `<td class="mono">${String(val).slice(0, 80)}</td>`;
          if (c === barCol) {
            cell = `<td><span class="mono">${Number(row[c]).toLocaleString()}</span><div class="bar-bg"><div class="bar-fill" style="width:${pct}%"></div></div></td>`;
          }
          return cell;
        }).join('') + '</tr>';
      }).join('');
    }
    toast(isAr ? `تم تحميل التقرير: ${rows.length} سجل` : `Report loaded: ${rows.length} rows`, 'ok');
  } catch (e) {
    document.getElementById('agg-meta').textContent = 'Error: ' + e;
    toast(currentLang === 'ar' ? 'فشل تحميل التقرير' : 'Aggregation failed', 'err');
  }

  btn.disabled = false;
  btn.innerHTML = I18N[currentLang].btn_load_report;
}

// ══════════════════════════════════════════════════════
//  MATERIALIZED VIEWS
// ══════════════════════════════════════════════════════
async function refreshMV(mode) {
  const btnId = mode === 'incremental' ? 'btn-mv-inc' : 'btn-mv-full';
  const btn = document.getElementById(btnId);
  btn.disabled = true;
  btn.innerHTML = '<span class="spin"></span> ' + (currentLang === 'ar' ? 'جارٍ التحديث…' : 'Refreshing…');
  termClear('mv-terminal', '');
  termLog('mv-terminal', `▶ POST /refresh-mv (mode: ${mode})`, 't-info');

  try {
    const r = await fetch('/refresh-mv', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mode })
    });
    const d = await r.json();
    termLog('mv-terminal', '✓ Response received:', 't-ok');
    termLog('mv-terminal', JSON.stringify(d, null, 2), 't-muted');
    toast(currentLang === 'ar' ? `اكتمل تحديث العروض المادية (${mode === 'incremental' ? 'تزايدي' : 'كامل'})` : `MV refresh (${mode}) complete`, 'ok');
    loadMVData();
    loadDailySales();
    loadTopProducts();
  } catch (e) {
    termLog('mv-terminal', 'Error: ' + e, 't-err');
    toast(currentLang === 'ar' ? 'فشل تحديث العرض المادي' : 'MV refresh failed', 'err');
  }

  btn.disabled = false;
  btn.innerHTML = mode === 'incremental' ? I18N[currentLang].btn_mv_inc : I18N[currentLang].btn_mv_full;
}

async function loadMVData() {
  const tbody = document.getElementById('mv-status-body');
  if (!tbody) return;
  tbody.innerHTML = `<tr><td colspan="4" style="color:var(--muted);text-align:center">${currentLang === 'ar' ? 'جارٍ التحميل…' : 'Loading…'}</td></tr>`;

  try {
    const [c1, c2] = await Promise.all([
      fetch('/materialized/daily-sales?limit=365').then(r => r.json()),
      fetch('/materialized/top-products?limit=100').then(r => r.json())
    ]);

    const isAr = currentLang === 'ar';
    tbody.innerHTML = `
      <tr>
        <td><strong class="mono">daily_sales_summary</strong></td>
        <td class="mono">${(c1.results || []).length} ${isAr ? 'يوم' : 'days'}</td>
        <td><span class="tag tag-cyan">${isAr ? 'تزايدي' : 'incremental'}</span></td>
        <td><span class="tag tag-green">✓ ${isAr ? 'جاهز' : 'READY'}</span></td>
      </tr>
      <tr>
        <td><strong class="mono">top_products_summary</strong></td>
        <td class="mono">${(c2.results || []).length} ${isAr ? 'منتج' : 'SKUs'}</td>
        <td><span class="tag tag-cyan">${isAr ? 'تزايدي' : 'incremental'}</span></td>
        <td><span class="tag tag-green">✓ ${isAr ? 'جاهز' : 'READY'}</span></td>
      </tr>
    `;
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="4" style="color:var(--red)">Error: ${e}</td></tr>`;
  }
}

async function loadDailySales() {
  try {
    const r = await fetch('/materialized/daily-sales?limit=30');
    const d = await r.json();
    const rows = d.results || [];
    const tbody = document.getElementById('daily-sales-body');
    if (!tbody) return;

    tbody.innerHTML = rows.length === 0
      ? `<tr><td colspan="4" style="color:var(--muted);text-align:center">${currentLang === 'ar' ? 'لا توجد بيانات' : 'No data'}</td></tr>`
      : rows.map(item => `
        <tr>
          <td class="mono">${item.date || item._id || '—'}</td>
          <td class="mono">${(item.orders_count || item.count || 0).toLocaleString()}</td>
          <td class="mono">${(item.total_sales || item.revenue || 0).toLocaleString()}</td>
          <td class="mono">${(item.avg_order_value || item.avg || 0).toLocaleString()}</td>
        </tr>`).join('');
    toast(currentLang === 'ar' ? `تم تحميل ملخص المبيعات: ${rows.length} يوم` : `Daily sales loaded — ${rows.length} days`, 'ok');
  } catch (e) {
    toast('Error: ' + e, 'err');
  }
}

async function loadTopProducts() {
  try {
    const r = await fetch('/materialized/top-products?limit=20');
    const d = await r.json();
    const rows = d.results || [];
    const tbody = document.getElementById('top-products-body');
    if (!tbody) return;

    tbody.innerHTML = rows.length === 0
      ? `<tr><td colspan="4" style="color:var(--muted);text-align:center">${currentLang === 'ar' ? 'لا توجد بيانات' : 'No data'}</td></tr>`
      : rows.map(item => `
        <tr>
          <td><strong>${item.product_name || item._id || '—'}</strong></td>
          <td class="mono">${(item.total_quantity || item.units_sold || 0).toLocaleString()}</td>
          <td class="mono">${(item.total_revenue || item.total_sales || 0).toLocaleString()}</td>
          <td class="mono">${(item.avg_price || 0).toLocaleString()}</td>
        </tr>`).join('');
    toast(currentLang === 'ar' ? 'تم جلب أفضل المنتجات' : 'Top products loaded', 'ok');
  } catch (e) {
    toast('Error: ' + e, 'err');
  }
}

// ══════════════════════════════════════════════════════
//  SCHEDULED JOBS & AUDIT LOGS
// ══════════════════════════════════════════════════════
async function triggerJob(name) {
  termClear('job-terminal', '');
  termLog('job-terminal', `▶ POST /jobs/${name}/run`, 't-info');
  termLog('job-terminal', currentLang === 'ar' ? '  جارٍ تنفيذ المهمة في الخلفية…' : '  Waiting for job completion…', 't-muted');

  try {
    const r = await fetch(`/jobs/${name}/run`, { method: 'POST' });
    const d = await r.json();
    termLog('job-terminal', '✓ Job completed!', 't-ok');
    termLog('job-terminal', '  Status   : ' + (d.status || '—'), 't-ok');
    termLog('job-terminal', '  Duration : ' + (d.duration_seconds || '—') + 's');
    termLog('job-terminal', '  Run ID   : ' + (d.run_id || d.job_run_id || '—'));
    toast(currentLang === 'ar' ? `اكتملت المهمة "${name}" بنجاح` : `Job "${name}" complete`, 'ok');
    loadJobs();
  } catch (e) {
    termLog('job-terminal', 'Error: ' + e, 't-err');
    toast(currentLang === 'ar' ? 'فشل تنفيذ المهمة: ' + e : 'Job failed: ' + e, 'err');
  }
}

async function loadJobs() {
  try {
    const r = await fetch('/jobs');
    const d = await r.json();
    const hist = d.recent_execution_history || [];
    const tbody = document.getElementById('jobs-tbody');
    if (!tbody) return;

    if (hist.length === 0) {
      tbody.innerHTML = `<tr><td colspan="5" style="color:var(--muted);text-align:center;padding:16px">${currentLang === 'ar' ? 'لا توجد سجلات تنفيذ حتى الآن' : 'No execution history yet'}</td></tr>`;
      return;
    }

    tbody.innerHTML = hist.map(j => `
      <tr>
        <td><strong class="mono">${j.job_name || '—'}</strong></td>
        <td><span class="tag tag-purple">${j.triggered_by || 'manual'}</span></td>
        <td class="mono" style="color:var(--muted)">${j.start_time || j.started_at || '—'}</td>
        <td class="mono">${j.duration_seconds || '—'}s</td>
        <td><span class="tag ${j.status === 'SUCCESS' || j.status === 'success' ? 'tag-green' : 'tag-red'}">${(j.status || '—').toUpperCase()}</span></td>
      </tr>`).join('');
  } catch (e) {
    const tbody = document.getElementById('jobs-tbody');
    if (tbody) tbody.innerHTML = `<tr><td colspan="5" style="color:var(--red)">Error: ${e}</td></tr>`;
  }
}

// ══════════════════════════════════════════════════════
//  INITIALIZATION
// ══════════════════════════════════════════════════════
window.addEventListener('DOMContentLoaded', () => {
  applyLanguage(currentLang);
  checkHealth();
  loadOverviewKPIs();
  onQuerySelectChange();
});
