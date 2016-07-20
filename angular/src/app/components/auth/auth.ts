import {Component} from "@angular/core";
import {Http, Request, RequestMethod, Headers, HTTP_PROVIDERS} from "@angular/http";
import {Router} from "@angular/router";
import {AuthManager} from "../../authmanager";
import {IUser, ICompany} from "../../interfaces";
import {Utility} from "../../utility";

@Component({
    selector: "auth",
    viewProviders: [HTTP_PROVIDERS, AuthManager, Utility],
    templateUrl: "./app/components/auth/auth.html"
})
export class AuthPage {

    http: Http;
    authManager: AuthManager;
    router: Router;
    companies: Array<ICompany>;
    userCompany: string;
    utility: Utility;

    constructor(http: Http, router: Router, authManager: AuthManager, utility: Utility) {
        this.router = router;
        this.authManager = authManager;
        this.http = http;
        this.utility = utility;
        this.companies = [];
        this.userCompany = "";
        this.utility.makeGetRequest("/api/company/getAll", []).then((result) => {
            this.companies = <Array<ICompany>> result;
        }, (error) => {
            console.error(error);
        });
    }

    login(username: string, password: string) {
        if (!username || username == "") {
            console.error("Username must exist");
        } else if (!password || password == "") {
            console.error("Password must exist");
        } else {
            this.authManager.login(username, password).then((result) => {
                this.router.navigate(["/"]);
            }, (error) => {
                console.error(error);
            });
        }
    }

    register(firstname: string, lastname: string, street: string, city: string, state: string, zip: string, country: string, phone: string, username: string, password: string, company: string) {
        var postBody: IUser = {
            name: {
                first: firstname,
                last: lastname
            },
            address: {
                street: street,
                city: city,
                state: state,
                zip: zip,
                country: country
            },
            username: username,
            phone: phone,
            password: password,
            company: company
        }
        this.authManager.register(postBody).then((result) => {
            this.authManager.login(username, password).then((result) => {
                this.router.navigate(["/"]);
            }, (error) => {
                console.error(error);
            });
        }, (error) => {
            console.error(error);
        });;
    }

}
